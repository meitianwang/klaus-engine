use crate::engine::code_mode::protocol::{ImageDetail, OutputContentItem};
use crate::runtime_values::{RuntimeJson, RuntimeString};

const IMAGE_HELPER_EXPECTS_MESSAGE: &str = "image expects a non-empty image URL string, an object with image_url and optional detail, or a raw MCP image block";
const REMOTE_IMAGE_URL_ERROR: &str = "Tool call failed: remote image URLs are not supported in tool outputs. Pass a base64 data URI instead";
const CODEX_IMAGE_DETAIL_META_KEY: &str = "codex/imageDetail";

pub(super) fn serialize_output_text(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<RuntimeString, RuntimeString> {
    if value.is_string()
        || value.is_undefined()
        || value.is_null()
        || value.is_boolean()
        || value.is_number()
        || value.is_big_int()
    {
        return v8_value_to_runtime_string(scope, value);
    }

    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    if let Some(stringified) = v8::json::stringify(&tc, value) {
        return v8_string_to_runtime(&tc, stringified);
    }
    if tc.has_caught() {
        return Err(tc
            .exception()
            .map(|exception| value_to_error_text(&mut tc, exception))
            .unwrap_or_else(|| RuntimeString::from("unknown code mode exception")));
    }
    v8_value_to_runtime_string(&tc, value)
}

pub(super) fn normalize_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
    detail_override: Option<RuntimeString>,
) -> Result<OutputContentItem, ()> {
    let result = (|| -> Result<OutputContentItem, RuntimeString> {
        let (image_url, detail) = if value.is_string() {
            (v8_value_to_runtime_string(scope, value)?, None)
        } else if value.is_object() && !value.is_array() {
            let object = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE))?;
            if let Some(image) = parse_non_mcp_output_image(scope, object)? {
                image
            } else {
                parse_mcp_output_image(scope, value)?
            }
        } else {
            return Err(RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE));
        };

        if image_url.is_empty() {
            return Err(RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE));
        }
        if starts_with_ascii_case_insensitive(&image_url, "http://")
            || starts_with_ascii_case_insensitive(&image_url, "https://")
        {
            return Err(RuntimeString::from(REMOTE_IMAGE_URL_ERROR));
        }

        let detail = detail_override.or(detail);
        let detail = match detail {
            Some(detail) => Some(parse_image_detail(&detail)?),
            None => Some(ImageDetail::Auto),
        };

        Ok(OutputContentItem::InputImage { image_url, detail })
    })();

    match result {
        Ok(item) => Ok(item),
        Err(error_text) => {
            throw_type_error(scope, &error_text);
            Err(())
        }
    }
}

fn parse_non_mcp_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    object: v8::Local<'_, v8::Object>,
) -> Result<Option<(RuntimeString, Option<RuntimeString>)>, RuntimeString> {
    let image_url_key = v8::String::new(scope, "image_url")
        .ok_or_else(|| RuntimeString::from("failed to allocate image helper keys"))?;
    let Some(image_url) = object.get(scope, image_url_key.into()) else {
        return Ok(None);
    };
    if image_url.is_undefined() {
        return Ok(None);
    }
    if !image_url.is_string() {
        return Err(RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE));
    }
    let detail_key = v8::String::new(scope, "detail")
        .ok_or_else(|| RuntimeString::from("failed to allocate image helper keys"))?;
    let detail = parse_image_detail_value(scope, object.get(scope, detail_key.into()))?;
    Ok(Some((
        v8_value_to_runtime_string(scope, image_url)?,
        detail,
    )))
}

fn parse_mcp_output_image(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<(RuntimeString, Option<RuntimeString>), RuntimeString> {
    let Some(result) = v8_value_to_json(scope, value)? else {
        return Err(RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE));
    };
    let result = result
        .as_object()
        .ok_or_else(|| RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE))?;
    let item_type = result
        .get("type")
        .and_then(RuntimeJson::as_string)
        .ok_or_else(|| RuntimeString::from(IMAGE_HELPER_EXPECTS_MESSAGE))?;
    if item_type != "image" {
        return Err(
            RuntimeString::from("image only accepts MCP image blocks, got \"")
                .concat(item_type)
                .concat(&RuntimeString::from("\"")),
        );
    }
    let data = result
        .get("data")
        .and_then(RuntimeJson::as_string)
        .ok_or_else(|| RuntimeString::from("image expected MCP image data"))?;
    if data.is_empty() {
        return Err(RuntimeString::from("image expected MCP image data"));
    }

    let image_url = if starts_with_ascii_case_insensitive(data, "data:") {
        data.clone()
    } else {
        let mime_type = result
            .get("mimeType")
            .or_else(|| result.get("mime_type"))
            .and_then(RuntimeJson::as_string)
            .filter(|mime_type| !mime_type.is_empty())
            .cloned()
            .unwrap_or_else(|| RuntimeString::from("application/octet-stream"));
        RuntimeString::from("data:")
            .concat(&mime_type)
            .concat(&RuntimeString::from(";base64,"))
            .concat(data)
    };
    let detail = result
        .get("_meta")
        .and_then(RuntimeJson::as_object)
        .and_then(|meta| meta.get(CODEX_IMAGE_DETAIL_META_KEY))
        .and_then(RuntimeJson::as_string)
        .filter(|detail| is_image_detail(detail))
        .cloned();
    Ok((image_url, detail))
}

fn parse_image_detail_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: Option<v8::Local<'s, v8::Value>>,
) -> Result<Option<RuntimeString>, RuntimeString> {
    match value {
        Some(value) if value.is_string() => Ok(Some(v8_value_to_runtime_string(scope, value)?)),
        Some(value) if value.is_null() || value.is_undefined() => Ok(None),
        Some(_) => Err(RuntimeString::from(
            "image detail must be a string when provided",
        )),
        None => Ok(None),
    }
}

fn parse_image_detail(detail: &RuntimeString) -> Result<ImageDetail, RuntimeString> {
    if eq_ascii_case_insensitive(detail, "auto") {
        Ok(ImageDetail::Auto)
    } else if eq_ascii_case_insensitive(detail, "low") {
        Ok(ImageDetail::Low)
    } else if eq_ascii_case_insensitive(detail, "high") {
        Ok(ImageDetail::High)
    } else if eq_ascii_case_insensitive(detail, "original") {
        Ok(ImageDetail::Original)
    } else {
        Err(RuntimeString::from(
            "image detail must be one of: auto, low, high, original",
        ))
    }
}

fn is_image_detail(detail: &RuntimeString) -> bool {
    detail == "auto" || detail == "low" || detail == "high" || detail == "original"
}

fn starts_with_ascii_case_insensitive(value: &RuntimeString, prefix: &str) -> bool {
    let prefix = prefix.as_bytes();
    value.as_utf16().len() >= prefix.len()
        && value.as_utf16()[..prefix.len()]
            .iter()
            .copied()
            .zip(prefix.iter().copied())
            .all(|(left, right)| left <= 0x7f && (left as u8).eq_ignore_ascii_case(&right))
}

fn eq_ascii_case_insensitive(value: &RuntimeString, expected: &str) -> bool {
    value.len_utf16() == expected.len() && starts_with_ascii_case_insensitive(value, expected)
}

pub(crate) fn v8_value_to_json(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<Option<RuntimeJson>, RuntimeString> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let mut tc = tc.init();
    let Some(stringified) = v8::json::stringify(&tc, value) else {
        if tc.has_caught() {
            return Err(tc
                .exception()
                .map(|exception| value_to_error_text(&mut tc, exception))
                .unwrap_or_else(|| RuntimeString::from("unknown code mode exception")));
        }
        return Ok(None);
    };
    let stringified = v8_string_to_runtime(&tc, stringified)?;
    crate::runtime_values::runtime_json::parse(&stringified)
        .map(Some)
        .map_err(|error| {
            RuntimeString::from("failed to serialize JavaScript value: ")
                .concat(&RuntimeString::from(error.to_string()))
        })
}

pub(crate) fn json_to_v8<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: &RuntimeJson,
) -> Option<v8::Local<'s, v8::Value>> {
    let json = crate::runtime_values::runtime_json::stringify(value);
    let json = runtime_string_to_v8(scope, &json)?;
    v8::json::parse(scope, json)
}

pub(crate) fn runtime_string_to_v8<'s>(
    scope: &v8::PinScope<'s, '_>,
    value: &RuntimeString,
) -> Option<v8::Local<'s, v8::String>> {
    v8::String::new_from_two_byte(scope, value.as_utf16(), v8::NewStringType::Normal)
}

pub(super) fn v8_string_to_runtime(
    scope: &v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::String>,
) -> Result<RuntimeString, RuntimeString> {
    let mut units = vec![0_u16; value.length()];
    value.write_v2(scope, 0, &mut units, v8::WriteFlags::default());
    Ok(RuntimeString::from_utf16(units))
}

pub(crate) fn v8_value_to_runtime_string(
    scope: &v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Result<RuntimeString, RuntimeString> {
    let value = value
        .to_string(scope)
        .ok_or_else(|| RuntimeString::from("failed to convert JavaScript value to string"))?;
    v8_string_to_runtime(scope, value)
}

pub(crate) fn value_to_error_text(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> RuntimeString {
    if value.is_object() {
        if let Ok(object) = v8::Local::<v8::Object>::try_from(value) {
            if let Some(key) = v8::String::new(scope, "message") {
                if let Some(message) = object.get(scope, key.into()) {
                    if let Ok(message) = v8_value_to_runtime_string(scope, message) {
                        return RuntimeString::from("Error: ").concat(&message);
                    }
                }
            }
        }
    }
    let message = v8_value_to_runtime_string(scope, value).unwrap_or_default();
    if message.starts_with("Error:") {
        message
    } else {
        RuntimeString::from("Error: ").concat(&message)
    }
}

pub(super) fn throw_type_error(scope: &mut v8::PinScope<'_, '_>, message: &RuntimeString) {
    if let Some(message) = runtime_string_to_v8(scope, message) {
        scope.throw_exception(message.into());
    }
}

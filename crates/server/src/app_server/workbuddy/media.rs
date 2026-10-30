//! Independent image and video operations backed by the account catalog.
use super::native::{self, NativeResponse, NativeSession, MAX_IMAGE, MAX_JSON, MAX_VIDEO};
use crate::app_server::wire::{
    WorkBuddyImageParams, WorkBuddyReferenceImage, WorkBuddyVideoParams,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) fn enabled(config: &Value, key: &str, default: bool) -> bool {
    config["productFeatures"][key].as_bool().unwrap_or(default)
}
fn multimodal(config: &Value) -> bool {
    !enabled(config, "DisableMultimodalGeneration", false)
}
fn models(config: &Value, tag: &str) -> Vec<Value> {
    let entries: Vec<Value> = config["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| {
            m["disabled"] != true
                && m["local"] != true
                && config["availableModels"]
                    .as_array()
                    .filter(|v| !v.is_empty())
                    .is_none_or(|v| v.iter().any(|id| id == &m["id"]))
                && m["id"]
                    .as_str()
                    .is_some_and(|id| !id.is_empty() && id.len() <= 256)
                && m["tags"]
                    .as_array()
                    .is_some_and(|tags| tags.iter().any(|v| v == tag))
        })
        .cloned()
        .collect();
    if !entries.is_empty()
        || config["models"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["tags"]
                    .as_array()
                    .is_some_and(|tags| tags.iter().any(|value| value == tag))
            })
        })
    {
        return entries;
    }
    let feature = match tag {
        "text-to-image" | "image-to-image" => "ImageGen",
        _ => "VideoGen",
    };
    if !multimodal(config) || !enabled(config, feature, false) {
        return Vec::new();
    }
    super::metadata::media_models(config, tag)
}
fn row(config: &Value, tag: &str, feature: &str, references: bool) -> Value {
    let models = models(config, tag);
    let available = multimodal(config)
        && (enabled(config, feature, false)
            || (feature == "ImageEdit" && enabled(config, "ImageGen", false)))
        && !models.is_empty();
    let entries:Vec<Value>=models.iter().map(|m|json!({"id":m["id"],"label":m["name"].as_str().or_else(||m["id"].as_str()).unwrap_or("")})).collect();
    let mut row = json!({"available":available,"models":entries,"referenceImages":references});
    if let Some(first) = models.first() {
        row["defaultModel"] = first["id"].clone();
    }
    if !available {
        row["reason"] = json!("workbuddy_capability_unavailable");
    }
    row
}
fn video_models(config: &Value) -> Vec<Value> {
    let mut all = models(config, "text-to-video");
    for model in models(config, "image-to-video") {
        if !all.iter().any(|m| m["id"] == model["id"]) {
            all.push(model);
        }
    }
    all
}
pub(crate) fn project_capabilities(config: &Value) -> Value {
    let audio = !enabled(config, "DisableVoiceOutput", false);
    let transcription = !enabled(config, "DisableVoiceInput", false);
    json!({"image":row(config,"text-to-image","ImageGen",!models(config,"image-to-image").is_empty()),"imageEdit":row(config,"image-to-image","ImageEdit",true),
        "video":{ "available":multimodal(config)&&enabled(config,"VideoGen",false)&&(!models(config,"text-to-video").is_empty()||!models(config,"image-to-video").is_empty()),
            "models":video_models(config).iter().map(|m|json!({"id":m["id"],"label":m["name"].as_str().or_else(||m["id"].as_str()).unwrap_or(""),"referenceImages":m["tags"].as_array().is_some_and(|tags|tags.iter().any(|v|v=="image-to-video"))})).collect::<Vec<_>>(),
            "defaultModel":models(config,"text-to-video").first().map(|m|m["id"].clone()).or_else(||models(config,"image-to-video").first().map(|m|m["id"].clone())),"referenceImages":!models(config,"image-to-video").is_empty()},
        "audio":{"available":audio,"voice":false,"speed":false},"transcription":{"available":transcription,"language":false,"streaming":transcription},
        "search":{"available":enabled(config,"WebSearch",true)},"fetch":{"available":enabled(config,"WebFetch",true)&&enabled(config,"WebFetchRemoteApi",true)},"unresolvedModels":super::metadata::unresolved(config)})
}
pub async fn capabilities(signal: AbortSignal) -> Result<NativeResponse, String> {
    native::bounded(&signal, 20, async {
        let session = NativeSession::start().await?;
        let config = session.config().await?;
        session.response(project_capabilities(&config))
    })
    .await
}
fn select(config: &Value, tag: &str, feature: &str, model: Option<&str>) -> Result<Value, String> {
    if !multimodal(config)
        || !(enabled(config, feature, false)
            || (feature == "ImageEdit" && enabled(config, "ImageGen", false)))
    {
        return Err("workbuddy_capability_unavailable".into());
    }
    let models = models(config, tag);
    let model = match model {
        Some(id) => models.iter().find(|m| m["id"] == id),
        None => models.first(),
    }
    .ok_or("workbuddy_media_model_unavailable")?;
    Ok(model.clone())
}
fn reference_images(
    images: &[WorkBuddyReferenceImage],
    limit: usize,
) -> Result<Vec<String>, String> {
    if images.len() > limit {
        return Err("workbuddy_reference_images_invalid".into());
    }
    let mut total = 0;
    let mut urls = Vec::new();
    for image in images {
        let bytes = native::decode(&image.data_base64, 4 * 1024 * 1024)?;
        total += bytes.len();
        if total > 8 * 1024 * 1024 || native::image_mime(&bytes) != Some(image.mime_type.as_str()) {
            return Err("workbuddy_reference_images_invalid".into());
        }
        urls.push(format!(
            "data:{};base64,{}",
            image.mime_type,
            STANDARD.encode(bytes)
        ));
    }
    Ok(urls)
}
pub async fn generate_image(
    params: WorkBuddyImageParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal, 180, async {
        native::text(&params.prompt, 16_384, "workbuddy_image_prompt_invalid")?;
        let references = reference_images(&params.reference_images, 4)?;
        let edit = !references.is_empty();
        let session = NativeSession::start().await?;
        let config = session.config().await?;
        let model = select(
            &config,
            if edit {
                "image-to-image"
            } else {
                "text-to-image"
            },
            if edit { "ImageEdit" } else { "ImageGen" },
            params.model.as_deref(),
        )?;
        let id = model["id"]
            .as_str()
            .ok_or("workbuddy_media_model_unavailable")?;
        let size = params.size.as_deref().unwrap_or("1024x1024");
        if size.len() > 32
            || size.is_empty()
            || !size
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'x' | b':' | b'_'))
        {
            return Err("workbuddy_image_size_invalid".into());
        }
        let mut body = json!({"model":id,"prompt":params.prompt,"size":size});
        if !id.starts_with("hunyuan-") && !id.starts_with("gemini-") {
            body["response_format"] = json!("b64_json");
            body["n"] = json!(1);
        }
        if id.starts_with("hunyuan-") {
            body["n"] = json!(1);
        }
        if edit {
            body["image"] = json!(references);
        }
        let path = if edit {
            "/v2/images/edits"
        } else {
            "/v2/images/generations"
        };
        let value = session
            .json(
                reqwest::Method::POST,
                path,
                Some(body),
                params.session_id.as_deref(),
                24 * 1024 * 1024,
            )
            .await?;
        let image = value["data"]["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or("workbuddy_image_response_invalid")?;
        let bytes = if let Some(data) = image["b64_json"].as_str() {
            native::decode(data, MAX_IMAGE)?
        } else if let Some(url) = image["url"].as_str() {
            session.download(url, MAX_IMAGE).await?
        } else {
            return Err("workbuddy_image_response_invalid".into());
        };
        let mime = native::image_mime(&bytes).ok_or("workbuddy_image_response_invalid")?;
        session.response(json!({"imageBase64":STANDARD.encode(&bytes),"mimeType":mime,"model":id}))
    })
    .await
}
fn video_size(size: Option<&str>, model: &Value, has_reference: bool) -> Result<Value, String> {
    let config = &model["videoGeneration"];
    let mut out = json!({"enable_audio":true});
    if let Some(resolution) = config["defaultResolution"].as_str() {
        out["resolution"] = json!(resolution);
    }
    if let Some(size) = size.filter(|size| *size != "auto") {
        if ["16:9", "9:16", "1:1"].contains(&size) {
            if !has_reference {
                out["aspect_ratio"] = json!(size);
            }
        } else if let Some(resolution) = config["resolutions"].as_array().and_then(|rows| {
            rows.iter()
                .filter_map(Value::as_str)
                .find(|r| r.eq_ignore_ascii_case(size))
        }) {
            out["resolution"] = json!(resolution);
        } else if let Some((w, h)) = size.split_once('x') {
            if w.is_empty()
                || h.is_empty()
                || !w.bytes().chain(h.bytes()).all(|b| b.is_ascii_digit())
            {
                return Err("workbuddy_video_size_unsupported".into());
            }
            let w: u32 = w.parse().map_err(|_| "workbuddy_video_size_unsupported")?;
            let h: u32 = h.parse().map_err(|_| "workbuddy_video_size_unsupported")?;
            if w == 0 || h == 0 || w > 16384 || h > 16384 {
                return Err("workbuddy_video_size_unsupported".into());
            }
            if !has_reference {
                let ratio = if w * 9 == h * 16 {
                    "16:9"
                } else if w * 16 == h * 9 {
                    "9:16"
                } else if w == h {
                    "1:1"
                } else {
                    return Err("workbuddy_video_size_unsupported".into());
                };
                out["aspect_ratio"] = json!(ratio);
            }
            let requested = format!("{}p", w.min(h));
            // Some catalogs expose quality modes instead of pixel resolutions.
            // Keep their default rather than forwarding an unsupported override.
            if let Some(resolution) = config["resolutions"].as_array().and_then(|rows| {
                rows.iter()
                    .filter_map(Value::as_str)
                    .find(|r| r.eq_ignore_ascii_case(&requested))
            }) {
                out["resolution"] = json!(resolution);
            }
        } else {
            return Err("workbuddy_video_size_unsupported".into());
        }
    }
    if !has_reference && out.get("aspect_ratio").is_none() {
        out["aspect_ratio"] = json!("16:9");
    }
    Ok(out)
}
fn video_task_id(value: &Value) -> Result<Value, String> {
    let task = &value["data"]["id"];
    if task
        .as_str()
        .is_some_and(|id| !id.trim().is_empty() && id.len() <= 512)
        || task.as_u64().is_some_and(|id| id > 0)
    {
        Ok(task.clone())
    } else {
        Err("workbuddy_video_response_invalid".into())
    }
}

fn video_task_completed(value: &Value) -> Result<bool, String> {
    match value["data"]["status"]
        .as_str()
        .filter(|status| !status.is_empty() && status.len() <= 64)
    {
        Some("completed") => Ok(true),
        Some("failed") => Err("workbuddy_video_failed".into()),
        Some(_) => Ok(false),
        None => Err("workbuddy_video_status_invalid".into()),
    }
}

pub async fn generate_video(
    params: WorkBuddyVideoParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal,870,async{
    native::text(&params.prompt,2500,"workbuddy_video_prompt_invalid")?;let references=reference_images(&params.reference_images,2)?;let session=NativeSession::start().await?;let config=session.config().await?;
    let model=select(&config,if references.is_empty(){"text-to-video"}else{"image-to-video"},"VideoGen",params.model.as_deref())?;
    let id=model["id"].as_str().ok_or("workbuddy_media_model_unavailable")?;let seconds=params.seconds.or_else(||model["videoGeneration"]["defaultSeconds"].as_u64().and_then(|n|u32::try_from(n).ok()));
    if let Some(seconds)=seconds{let min=model["videoGeneration"]["minSeconds"].as_u64();let max=model["videoGeneration"]["maxSeconds"].as_u64();if seconds==0||seconds>60||params.seconds.is_some()&&(min.is_none()||max.is_none())||min.is_some_and(|m|u64::from(seconds)<m)||max.is_some_and(|m|u64::from(seconds)>m){return Err("workbuddy_video_duration_unsupported".into());}}
    let mut body=json!({"model":id,"prompt":params.prompt,"negative_prompt":"","watermark":true,"extra_parameters":video_size(params.size.as_deref(),&model,!references.is_empty())?});
    if let Some(seconds)=seconds{body["seconds"]=json!(seconds);}
    if let Some(image)=references.first(){body["image_url"]=json!(image);}
    if let Some(image)=references.get(1){body["last_image_url"]=json!(image);}
    let submitted=session.json(reqwest::Method::POST,"/v2/videos/generations",Some(body),params.session_id.as_deref(),MAX_JSON).await?;
    let task = video_task_id(&submitted)?;
    let finished=tokio::time::timeout(Duration::from_secs(600),async{loop{session.check()?;let value=session.json(reqwest::Method::POST,"/v2/videos/tasks",Some(json!({"task_id":task})),params.session_id.as_deref(),MAX_JSON).await?;
        if video_task_completed(&value)? { break Ok::<Value,String>(value); }tokio::time::sleep(Duration::from_secs(5)).await;}}).await.map_err(|_|"workbuddy_operation_timed_out")??;
    let url=finished["data"]["data"].as_array().and_then(|v|v.first()).and_then(|v|v["url"].as_str()).ok_or("workbuddy_video_response_invalid")?;
    let bytes=tokio::time::timeout(Duration::from_secs(120),session.download(url,MAX_VIDEO)).await.map_err(|_|"workbuddy_operation_timed_out")??;if bytes.get(4..8)!=Some(b"ftyp"){return Err("workbuddy_video_response_invalid".into());}
    session.response(json!({"videoBase64":STANDARD.encode(bytes),"mimeType":"video/mp4","model":id}))
}).await
}

#[cfg(test)]
#[path = "media_tests.rs"]
mod tests;

//! Exact runtime carriers for trusted goal-owned preparation metadata.

use super::{GoalOwnedResourceFile, GoalOwnedResourcesInput, GoalOwnedResourcesManifest};
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeCodecErrorKind, RuntimeDecode, RuntimeEncode,
    RuntimeEncoder, RuntimeJson, RuntimeObjectDecoder, RuntimeObjectEncoder, RuntimeString,
};

fn scalar_field(
    object: &mut RuntimeObjectDecoder<'_>,
    field: &str,
) -> Result<String, RuntimeCodecError> {
    let value: RuntimeString = object.required(field, &[])?;
    value.try_to_utf8().map_err(|error| {
        RuntimeCodecError::new(RuntimeCodecErrorKind::Custom(error.to_string().into()))
            .prepend(field)
    })
}

impl RuntimeEncode for GoalOwnedResourceFile {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("name", &self.name)?;
        object.field("sizeBytes", &self.size_bytes)?;
        object.field("sha256", &self.sha256)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GoalOwnedResourceFile {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                name: scalar_field(object, "name")?,
                size_bytes: object.required("sizeBytes", &[])?,
                sha256: scalar_field(object, "sha256")?,
            })
        })
    }
}

impl RuntimeEncode for GoalOwnedResourcesInput {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("preparationId", &self.preparation_id)?;
        object.field("files", &self.files)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GoalOwnedResourcesInput {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                preparation_id: scalar_field(object, "preparationId")?,
                files: object.required("files", &[])?,
            })
        })
    }
}

impl RuntimeEncode for GoalOwnedResourcesManifest {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        let mut object = RuntimeObjectEncoder::new();
        object.field("sourceThreadId", &self.source_thread_id)?;
        object.field("preparationId", &self.preparation_id)?;
        object.field("files", &self.files)?;
        out.emit(RuntimeJson::Object(object.finish()))
    }
}

impl RuntimeDecode for GoalOwnedResourcesManifest {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        runtime_codec::decode_object(value, true, |object| {
            Ok(Self {
                source_thread_id: scalar_field(object, "sourceThreadId")?,
                preparation_id: scalar_field(object, "preparationId")?,
                files: object.required("files", &[])?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_values::runtime_json;

    fn resource_json() -> RuntimeJson {
        runtime_json::parse(&RuntimeString::from(r#"{"preparationId":"6c140adc-9de2-4e68-89c5-0ef0403a2ea1","files":[{"name":"objective.md","sizeBytes":12,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}"#)).unwrap()
    }

    #[test]
    fn input_round_trip_keeps_only_host_preparation_metadata() {
        let json = resource_json();
        let decoded = GoalOwnedResourcesInput::decode_runtime(&json).unwrap();
        assert_eq!(decoded.files[0].size_bytes, 12);
        assert_eq!(runtime_codec::encode_value(&decoded).unwrap(), json);
        let manifest = GoalOwnedResourcesManifest {
            source_thread_id: "engine-bound-thread".to_owned(),
            preparation_id: decoded.preparation_id,
            files: decoded.files,
        };
        let manifest_json = runtime_codec::encode_value(&manifest).unwrap();
        assert_eq!(
            GoalOwnedResourcesManifest::decode_runtime(&manifest_json).unwrap(),
            manifest
        );
        assert!(
            GoalOwnedResourcesInput::decode_runtime(&manifest_json).is_err(),
            "a host cannot choose the manifest source thread"
        );
    }

    #[test]
    fn input_rejects_paths_unknown_fields_fractional_sizes_and_non_scalar_names() {
        for json in [
            r#"{"preparationId":"p","files":[],"path":"/tmp/anything"}"#,
            r#"{"preparationId":"p","sourceThreadId":"host-chosen","files":[]}"#,
            r#"{"preparationId":"p","files":[{"name":"objective.md","sizeBytes":12,"sha256":"a","path":"/tmp/anything"}]}"#,
            r#"{"preparationId":"p","files":[{"name":"objective.md","sizeBytes":1.5,"sha256":"a"}]}"#,
            r#"{"preparationId":"p","files":[{"name":"objective.md","sizeBytes":-1,"sha256":"a"}]}"#,
            r#"{"preparationId":"p","files":[{"name":"\ud800","sizeBytes":12,"sha256":"a"}]}"#,
        ] {
            let value = runtime_json::parse(&RuntimeString::from(json)).unwrap();
            assert!(
                GoalOwnedResourcesInput::decode_runtime(&value).is_err(),
                "accepted {json}"
            );
        }
    }
}

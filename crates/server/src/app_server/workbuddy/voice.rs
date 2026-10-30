//! Finite speech synthesis and transcription services.
use super::{
    media,
    native::{self, NativeResponse, NativeSession, MAX_AUDIO, MAX_JSON},
};
use crate::app_server::wire::{WorkBuddySpeechParams, WorkBuddyTranscribeParams};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::StreamExt;
use klaus_agent_core::types::abort::AbortSignal;
use serde_json::json;

#[derive(Default)]
struct SpeechStream {
    pending: Vec<u8>,
    event: String,
    data: String,
    pcm: Vec<u8>,
}
impl SpeechStream {
    fn event(&mut self) -> Result<(), String> {
        if self.event == "error" {
            return Err("workbuddy_audio_generation_failed".into());
        }
        if self.event == "audio" && !self.data.is_empty() {
            let data: String = self
                .data
                .chars()
                .filter(|c| !c.is_ascii_whitespace())
                .collect();
            let bytes = native::decode(&data, MAX_AUDIO)?;
            if bytes.len() > MAX_AUDIO.saturating_sub(44).saturating_sub(self.pcm.len()) {
                return Err("workbuddy_audio_too_large".into());
            }
            self.pcm.extend(bytes);
        }
        self.event.clear();
        self.data.clear();
        Ok(())
    }
    fn line(&mut self, line: &[u8]) -> Result<(), String> {
        let line = std::str::from_utf8(line)
            .map_err(|_| "workbuddy_audio_stream_invalid")?
            .trim_end_matches('\r');
        if line.is_empty() {
            return self.event();
        }
        if let Some(event) = line.strip_prefix("event:") {
            if event.len() > 64 {
                return Err("workbuddy_audio_stream_invalid".into());
            }
            self.event = event.trim().to_string();
        }
        if let Some(data) = line.strip_prefix("data:") {
            let data = data.strip_prefix(' ').unwrap_or(data);
            if self.data.len() + data.len() > 1024 * 1024 {
                return Err("workbuddy_audio_frame_too_large".into());
            }
            self.data.push_str(data);
        }
        Ok(())
    }
    fn feed(&mut self, bytes: &[u8]) -> Result<(), String> {
        for byte in bytes {
            if *byte == b'\n' {
                let line = std::mem::take(&mut self.pending);
                self.line(&line)?;
            } else {
                if self.pending.len() >= 1024 * 1024 {
                    return Err("workbuddy_audio_frame_too_large".into());
                }
                self.pending.push(*byte);
            }
        }
        Ok(())
    }
    fn finish(mut self) -> Result<Vec<u8>, String> {
        if !self.pending.is_empty() {
            return Err("workbuddy_audio_stream_truncated".into());
        }
        self.event()?;
        if self.pcm.is_empty() || !self.pcm.len().is_multiple_of(2) {
            return Err("workbuddy_audio_stream_invalid".into());
        }
        Ok(wav(&self.pcm))
    }
}
fn wav(pcm: &[u8]) -> Vec<u8> {
    let n = pcm.len() as u32;
    let mut out = Vec::with_capacity(pcm.len() + 44);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(n + 36).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&16000u32.to_le_bytes());
    out.extend_from_slice(&32000u32.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(pcm);
    out
}
fn speech_segments(mut text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    text = text.trim();
    while text.len() > 1000 {
        let mut end = 1000;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let prefix = &text[..end];
        let split = prefix
            .char_indices()
            .rev()
            .find(|(_, c)| matches!(c, '。' | '！' | '？' | '!' | '?' | '\n'))
            .or_else(|| {
                prefix
                    .char_indices()
                    .rev()
                    .find(|(_, c)| matches!(c, '，' | ',' | '；' | ';' | '：' | ':'))
            })
            .or_else(|| prefix.char_indices().rev().find(|(_, c)| c.is_whitespace()))
            .map(|(i, c)| i + c.len_utf8())
            .filter(|end| *end > 0)
            .unwrap_or(end);
        out.push(&text[..split]);
        text = text[split..].trim_start();
    }
    if !text.is_empty() {
        out.push(text);
    }
    out
}
pub async fn synthesize_speech(
    params: WorkBuddySpeechParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal, 120, async {
        native::text(&params.text, 16_384, "workbuddy_audio_text_invalid")?;
        if params.voice.is_some() || params.speed.is_some() {
            return Err("workbuddy_audio_options_unsupported".into());
        }
        let session = NativeSession::start().await?;
        let config = session.config().await?;
        if media::enabled(&config, "DisableVoiceOutput", false) {
            return Err("workbuddy_capability_unavailable".into());
        }
        let mut parser = SpeechStream::default();
        let mut consumed = 0usize;
        for segment in speech_segments(&params.text) {
            let response = session
                .request(reqwest::Method::POST, "/agenttool/v1/tts", None)?
                .json(&json!({"text":segment}))
                .send()
                .await
                .map_err(|_| "workbuddy_network_error")?;
            if !response.status().is_success() {
                let status = response.status().as_u16();
                let value = native::read_bytes(response, MAX_JSON).await?;
                let value = serde_json::from_slice(&value).unwrap_or(json!({}));
                native::check_error(status, &value)?;
                return Err("workbuddy_audio_generation_failed".into());
            }
            if !response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .is_some_and(|s| s.starts_with("text/event-stream"))
            {
                return Err("workbuddy_audio_stream_invalid".into());
            }
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| "workbuddy_response_read_failed")?;
                consumed += chunk.len();
                if consumed > MAX_AUDIO * 2 {
                    return Err("workbuddy_audio_too_large".into());
                }
                parser.feed(&chunk)?;
                session.check()?;
            }

            if !parser.pending.is_empty() || !parser.event.is_empty() || !parser.data.is_empty() {
                return Err("workbuddy_audio_stream_truncated".into());
            }
        }
        let bytes = parser.finish()?;
        session.response(json!({"audioBase64":STANDARD.encode(bytes),"mimeType":"audio/wav"}))
    })
    .await
}
fn audio_mime(bytes: &[u8], requested: Option<&str>) -> Result<&'static str, String> {
    let found = if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE") {
        "audio/wav"
    } else if bytes.starts_with(b"ID3")
        || bytes
            .get(..2)
            .is_some_and(|h| h[0] == 255 && h[1] & 0xe0 == 0xe0)
    {
        "audio/mpeg"
    } else if bytes.starts_with(b"OggS") {
        "audio/ogg"
    } else if bytes.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
        "audio/webm"
    } else if bytes.starts_with(b"fLaC") {
        "audio/flac"
    } else if bytes.get(4..8) == Some(b"ftyp") {
        "audio/mp4"
    } else {
        return Err("workbuddy_audio_input_invalid".into());
    };
    if requested.is_some_and(|mime| mime.len() > 128 || mime.chars().any(char::is_control)) {
        return Err("workbuddy_audio_mime_mismatch".into());
    }
    let requested = requested.map(|mime| {
        mime.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
    });
    if requested.as_deref().is_some_and(|mime| {
        mime != found
            && !(mime == "audio/mp3" && found == "audio/mpeg")
            && !(mime == "audio/x-wav" && found == "audio/wav")
    }) {
        return Err("workbuddy_audio_mime_mismatch".into());
    }
    Ok(found)
}
pub async fn transcribe_audio(
    params: WorkBuddyTranscribeParams,
    signal: AbortSignal,
) -> Result<NativeResponse, String> {
    native::bounded(&signal,90,async{
    if params.language.is_some(){return Err("workbuddy_transcription_language_unsupported".into());}
    let bytes=native::decode(&params.audio_base64,MAX_AUDIO)?;let mime=audio_mime(&bytes,params.mime_type.as_deref())?;
    let session=NativeSession::start_expected(params.expected_credential_generation.as_deref()).await?;let config=session.config().await?;if media::enabled(&config,"DisableVoiceInput",false){return Err("workbuddy_capability_unavailable".into());}
    let boundary=format!("klaus{}",uuid::Uuid::new_v4().simple());let ext=match mime{"audio/wav"=>"wav","audio/mpeg"=>"mp3","audio/ogg"=>"ogg","audio/flac"=>"flac","audio/mp4"=>"m4a",_=>"webm"};
    let mut body=format!("--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"audio.{ext}\"\r\nContent-Type: {mime}\r\n\r\n").into_bytes();body.extend_from_slice(&bytes);body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let response=session.request(reqwest::Method::POST,"/agenttool/v1/asr",None)?.header(reqwest::header::CONTENT_TYPE,format!("multipart/form-data; boundary={boundary}")).body(body).send().await.map_err(|_|"workbuddy_network_error")?;
    let status=response.status().as_u16();if matches!(status,401|403|429){native::check_error(status,&serde_json::Value::Null)?;}let bytes=native::read_bytes(response,MAX_JSON).await?;let result:serde_json::Value=serde_json::from_slice(&bytes).map_err(|_|"workbuddy_invalid_response")?;native::check_error(status,&result)?;
    let text=result["text"].as_str().filter(|s|s.len()<=128*1024).ok_or("workbuddy_transcription_invalid")?;session.response(json!({"text":text}))
}).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_sse_chunks_produce_bounded_pcm_wav() {
        let mut p = SpeechStream::default();
        p.feed(b"event: audio\ndata: AQ").unwrap();
        p.feed(b"ACAA==\n\n").unwrap();
        let wav = p.finish().unwrap();
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[44..], &[1, 0, 2, 0]);
        assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16000);
    }
    #[test]
    fn errors_and_truncated_audio_fail_closed() {
        let mut p = SpeechStream::default();
        assert!(p.feed(b"event: error\ndata: private error\n\n").is_err());
        let mut p = SpeechStream::default();
        p.feed(b"event: audio\ndata: AQA=").unwrap();
        assert!(p.finish().is_err());
    }
    #[test]
    fn audio_mime_matches_actual_bytes() {
        assert_eq!(
            audio_mime(&wav(&[0, 0]), Some("audio/wav")).unwrap(),
            "audio/wav"
        );
        assert!(audio_mime(&wav(&[0, 0]), Some("audio/mpeg")).is_err());
    }
    #[test]
    fn speech_segments_bound_utf8_without_dropping_text() {
        let text = "汉字".repeat(800);
        let parts = speech_segments(&text);
        assert!(parts.iter().all(|p| p.len() <= 1000));
        assert_eq!(parts.concat(), text);
    }
    #[test]
    fn recorder_codec_parameters_preserve_matching_webm_input() {
        assert_eq!(
            audio_mime(&[0x1a, 0x45, 0xdf, 0xa3], Some("audio/webm;codecs=opus")).unwrap(),
            "audio/webm"
        );
        assert!(audio_mime(&[0x1a, 0x45, 0xdf, 0xa3], Some("audio/wav;codecs=opus")).is_err());
    }
}

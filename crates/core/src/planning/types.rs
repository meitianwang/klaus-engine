//! Authoritative collaborative planning state and exact wire identities.
use crate::runtime_values::{
    runtime_codec, RuntimeCodecError, RuntimeDecode, RuntimeEncode, RuntimeEncoder, RuntimeJson,
    RuntimeObjectEncoder, RuntimeString,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CollaborationMode {
    #[default]
    Default,
    Plan,
}
impl CollaborationMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "default" | "code" | "pair_programming" | "execute" | "custom" => Ok(Self::Default),
            "plan" => Ok(Self::Plan),
            _ => Err("Unknown collaboration mode".into()),
        }
    }
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
        }
    }
}
impl RuntimeEncode for CollaborationMode {
    fn encode_runtime(&self, out: &mut RuntimeEncoder) -> Result<(), RuntimeCodecError> {
        self.wire_name().encode_runtime(out)
    }
}
impl RuntimeDecode for CollaborationMode {
    fn decode_runtime(value: &RuntimeJson) -> Result<Self, RuntimeCodecError> {
        let s = RuntimeString::decode_runtime(value)?;
        Self::parse(&String::from_utf8_lossy(&s.to_node_utf8())).map_err(|e| {
            RuntimeCodecError::new(crate::runtime_values::RuntimeCodecErrorKind::Custom(
                e.into(),
            ))
        })
    }
}

macro_rules! record_decode {
    ($obj:ident,review_pending,$wire:literal) => {
        $obj.defaulted($wire, &[], || true)?
    };
    ($obj:ident,$field:ident,$wire:literal) => {
        $obj.required($wire, &[])?
    };
}
macro_rules! record {
 ($name:ident {$($field:ident:$ty:ty => $wire:literal),* $(,)?})=>{
  #[derive(Debug,Clone,PartialEq)] pub struct $name {$(pub $field:$ty),*}
  impl RuntimeEncode for $name {fn encode_runtime(&self,out:&mut RuntimeEncoder)->Result<(),RuntimeCodecError>{let mut obj=RuntimeObjectEncoder::new();$(obj.field($wire,&self.$field)?;)*out.emit(RuntimeJson::Object(obj.finish()))}}
  impl RuntimeDecode for $name {fn decode_runtime(value:&RuntimeJson)->Result<Self,RuntimeCodecError>{runtime_codec::decode_object(value,false,|obj|Ok(Self{$($field:record_decode!(obj,$field,$wire),)*}))}}
 }
}
record!(ProposedPlan {id:RuntimeString=>"id",thread_id:RuntimeString=>"threadId",turn_id:RuntimeString=>"turnId",revision:u64=>"revision",content:RuntimeString=>"content",status:RuntimeString=>"status",review_pending:bool=>"reviewPending",document_path:Option<RuntimeString> =>"documentPath"});
record!(ImplementationReceipt {client_request_id:RuntimeString=>"clientRequestId",plan_id:RuntimeString=>"planId",revision:u64=>"revision",input:RuntimeString=>"input",implementation_id:RuntimeString=>"implementationId",kind:RuntimeString=>"kind",admission:RuntimeString=>"admission"});
record!(PlanningSnapshot {mode:CollaborationMode=>"mode",revision:u64=>"revision",plans:Vec<ProposedPlan> =>"plans",progress:Option<RuntimeJson> =>"progress",implementation:Option<ImplementationReceipt> =>"implementation",questions:Vec<RuntimeJson> =>"questions"});
impl Default for PlanningSnapshot {
    fn default() -> Self {
        Self {
            mode: CollaborationMode::Default,
            revision: 0,
            plans: Vec::new(),
            progress: None,
            implementation: None,
            questions: Vec::new(),
        }
    }
}

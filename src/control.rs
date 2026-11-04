use crate::error::IpcError;
use fory::{Fory, ForyEnum, ForyStruct};
use serde::{Deserialize, Serialize};

pub const FORY_ID_REGISTRATION: u32 = 1;
pub const FORY_ID_HEARTBEAT: u32 = 2;
pub const FORY_ID_DISPATCH: u32 = 3;
pub const FORY_ID_COMPLETION: u32 = 4;
pub const FORY_ID_CANCEL: u32 = 5;
pub const FORY_ID_WORKER_SHUTDOWN: u32 = 6;
pub const FORY_ID_STAGE_STARTED: u32 = 7;
pub const FORY_ID_CEILING_REQ: u32 = 8;
pub const FORY_ID_CEILING_RESP: u32 = 9;
pub const FORY_ID_SANDBOX_BACKEND_KIND: u32 = 10;
pub const FORY_ID_NETWORK_EGRESS_MODE: u32 = 11;
pub const FORY_ID_SANDBOX_CONSTRAINT_SPEC: u32 = 12;
pub const FORY_ID_SECURITY_GRANT_DENIED: u32 = 13;
pub const FORY_ID_TYPED_TASK_INVOCATION: u32 = 14;
pub const FORY_ID_SCCACHE_MODE: u32 = 15;

#[derive(ForyEnum, Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum SccacheMode {
    Off,
    #[default]
    Try,
    Mandatory,
}

impl Serialize for SccacheMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Off => serializer.serialize_str("false"),
            Self::Try => serializer.serialize_str("try"),
            Self::Mandatory => serializer.serialize_str("mandatory"),
        }
    }
}

impl<'de> Deserialize<'de> for SccacheMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SccacheModeVisitor;

        impl<'de> serde::de::Visitor<'de> for SccacheModeVisitor {
            type Value = SccacheMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a boolean (true/false) or string ('try', 'mandatory', 'true', 'false', 'off')",
                )
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v {
                    Ok(SccacheMode::Try)
                } else {
                    Ok(SccacheMode::Off)
                }
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v.trim().to_ascii_lowercase().as_str() {
                    "false" | "off" | "disabled" | "no" | "0" => Ok(SccacheMode::Off),
                    "true" | "try" | "auto" | "yes" | "1" => Ok(SccacheMode::Try),
                    "mandatory" | "required" | "strict" => Ok(SccacheMode::Mandatory),
                    other => Err(E::custom(format!(
                        "invalid sccache mode '{other}', expected true, false, try, or mandatory"
                    ))),
                }
            }
        }

        deserializer.deserialize_any(SccacheModeVisitor)
    }
}

#[derive(ForyEnum, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub enum SandboxBackendKind {
    #[default]
    Kernel,
    Hypervisor,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub enum NetworkEgressMode {
    #[default]
    Offline,
    RestrictedCidrs(Vec<String>),
}

impl fory::Serializer for NetworkEgressMode {
    type Target = Self;

    fn write_data(value: &Self, context: &mut fory::WriteContext) -> Result<(), fory::Error> {
        match value {
            Self::Offline => {
                <u8 as fory::Serializer>::write_data(&0u8, context)?;
            }
            Self::RestrictedCidrs(cidrs) => {
                <u8 as fory::Serializer>::write_data(&1u8, context)?;
                <Vec<String> as fory::Serializer>::write_data(cidrs, context)?;
            }
        }
        Ok(())
    }

    fn read_data(context: &mut fory::ReadContext) -> Result<Self, fory::Error> {
        let tag = <u8 as fory::Serializer>::read_data(context)?;
        match tag {
            0 => Ok(Self::Offline),
            1 => {
                let cidrs = <Vec<String> as fory::Serializer>::read_data(context)?;
                Ok(Self::RestrictedCidrs(cidrs))
            }
            other => Err(fory::Error::invalid_data(format!(
                "Invalid NetworkEgressMode tag: {other}"
            ))),
        }
    }

    fn default_value(_context: &mut fory::ReadContext) -> Result<Self, fory::Error> {
        Ok(Self::Offline)
    }
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub struct SandboxConstraintSpec {
    pub profile_name: String,
    pub backend: SandboxBackendKind,
    pub read_paths: Vec<String>,
    pub write_paths: Vec<String>,
    pub exec_paths: Vec<String>,
    pub network_mode: NetworkEgressMode,
    pub memlock_limit_kb: u64,
    pub max_pids: u32,
    pub require_set_ai_thread: bool,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SecurityGrantDenied {
    pub job_id: String,
    pub stage_id: String,
    pub violated_layer: String,
    pub offending_field: String,
    pub requested_value: String,
    pub ceiling_value: String,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct HostPrivilegeCeilingRequest {
    pub request_id: String,
    pub controller_version: u16,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct HostPrivilegeCeilingResponse {
    pub request_id: String,
    pub node_id: String,
    pub ceiling: SandboxConstraintSpec,
    pub kvm_available: bool,
    pub spacemit_ai_thread_available: bool,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct TypedTaskInvocation {
    pub task_id: String,
    pub tool_name: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub env_vars: Vec<(String, String)>,
    pub sandbox: SandboxConstraintSpec,
    #[serde(default)]
    pub sccache_mode: SccacheMode,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct WorkerRegistration {
    pub node_id: String,
    pub arch: String,
    pub tags: Vec<String>,
    pub max_slots: u32,
    pub ram_total_mb: u64,
    pub btrfs_enabled: bool,
    pub toolchains: Vec<String>,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct WorkerHeartbeat {
    pub node_id: String,
    pub active_slots: u32,
    pub cpu_load_pct: f32,
    pub ram_used_mb: u64,
    pub timestamp_epoch_ms: u64,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct StageDispatchTask {
    pub job_id: String,
    pub stage_id: String,
    pub stage_name: String,
    pub repo_name: String,
    pub clone_url: String,
    pub commit_sha: String,
    pub command: String,
    pub working_dir: Option<String>,
    pub env_vars: Vec<(String, String)>,
    pub timeout_seconds: u64,
    pub threads: u32,
    pub toolchain: Option<String>,
    pub exclusive: bool,
    pub max_concurrent_per_node: u32,
    pub tasks: Vec<TypedTaskInvocation>,
    pub sandbox_constraints: SandboxConstraintSpec,
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub sccache_mode: SccacheMode,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct StageCompletionReport {
    pub job_id: String,
    pub stage_id: String,
    pub exit_code: i32,
    pub duration_secs: u64,
    pub error_message: Option<String>,
    pub security_grant_denied: Option<SecurityGrantDenied>,
    #[serde(default)]
    pub traceparent: Option<String>,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct StageCancelTask {
    pub job_id: String,
    pub stage_id: String,
    pub reason: String,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct WorkerShutdown {
    pub node_id: String,
    pub reason: String,
    pub interrupted_stages: Vec<String>,
}

#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct StageStarted {
    pub job_id: String,
    pub stage_id: String,
    pub node_id: String,
    pub pid: u32,
    pub started_at_epoch_ms: u64,
    #[serde(default)]
    pub traceparent: Option<String>,
}

pub fn create_fory_runtime() -> Result<Fory, IpcError> {
    let mut fory = Fory::builder().xlang(false).build();
    fory.register::<WorkerRegistration>(FORY_ID_REGISTRATION)?;
    fory.register::<WorkerHeartbeat>(FORY_ID_HEARTBEAT)?;
    fory.register::<StageDispatchTask>(FORY_ID_DISPATCH)?;
    fory.register::<StageCompletionReport>(FORY_ID_COMPLETION)?;
    fory.register::<StageCancelTask>(FORY_ID_CANCEL)?;
    fory.register::<WorkerShutdown>(FORY_ID_WORKER_SHUTDOWN)?;
    fory.register::<StageStarted>(FORY_ID_STAGE_STARTED)?;
    fory.register::<HostPrivilegeCeilingRequest>(FORY_ID_CEILING_REQ)?;
    fory.register::<HostPrivilegeCeilingResponse>(FORY_ID_CEILING_RESP)?;
    fory.register::<SandboxBackendKind>(FORY_ID_SANDBOX_BACKEND_KIND)?;
    fory.register_serializer::<NetworkEgressMode>(FORY_ID_NETWORK_EGRESS_MODE)?;
    fory.register::<SandboxConstraintSpec>(FORY_ID_SANDBOX_CONSTRAINT_SPEC)?;
    fory.register::<SecurityGrantDenied>(FORY_ID_SECURITY_GRANT_DENIED)?;
    fory.register::<TypedTaskInvocation>(FORY_ID_TYPED_TASK_INVOCATION)?;
    fory.register::<SccacheMode>(FORY_ID_SCCACHE_MODE)?;
    Ok(fory)
}

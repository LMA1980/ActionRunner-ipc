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

/// Registration payload sent by a worker node upon connecting to the controller.
#[derive(ForyStruct, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct WorkerRegistration {
    pub node_id: String,
    pub arch: String,
    pub tags: Vec<String>,
    pub max_slots: u32,
    pub ram_total_mb: u64,
    pub btrfs_enabled: bool,
    pub toolchains: Vec<String>,
    /// Active Rust compiler semver string reported by the worker (for example, `"1.86.0"`).
    ///
    /// Uses `#[serde(default)]` and compatible Fory schema evolution so older workers
    /// without toolchain semver reporting continue to register as `None`.
    #[serde(default)]
    pub rust_semver: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(ForyStruct, Debug, PartialEq, Clone)]
    struct LegacyWorkerRegistration {
        node_id: String,
        arch: String,
        tags: Vec<String>,
        max_slots: u32,
        ram_total_mb: u64,
        btrfs_enabled: bool,
        toolchains: Vec<String>,
    }

    fn sample_registration(rust_semver: Option<&str>) -> WorkerRegistration {
        WorkerRegistration {
            node_id: "worker-riscv64".to_string(),
            arch: "riscv64".to_string(),
            tags: vec!["riscv64".to_string(), "test".to_string()],
            max_slots: 1,
            ram_total_mb: 32768,
            btrfs_enabled: true,
            toolchains: vec!["stable".to_string(), "nightly".to_string()],
            rust_semver: rust_semver.map(str::to_string),
        }
    }

    #[test]
    fn worker_registration_fory_and_json_roundtrip() {
        let fory = create_fory_runtime().expect("Fory runtime");
        for semver in [Some("1.86.0"), Some("1.88.0-nightly"), None] {
            let reg = sample_registration(semver);

            let fory_bytes = fory.serialize(&reg).expect("Fory serialize");
            let fory_decoded: WorkerRegistration =
                fory.deserialize(&fory_bytes).expect("Fory deserialize");
            assert_eq!(reg, fory_decoded);

            let json = serde_json::to_string(&reg).expect("JSON serialize");
            let json_decoded: WorkerRegistration =
                serde_json::from_str(&json).expect("JSON deserialize");
            assert_eq!(reg, json_decoded);
        }
    }

    #[test]
    fn worker_registration_json_omitted_rust_semver_defaults_to_none() {
        let legacy_json = r#"{
            "node_id": "node-a",
            "arch": "x86_64",
            "tags": ["x86_64", "clippy"],
            "max_slots": 1,
            "ram_total_mb": 16384,
            "btrfs_enabled": false,
            "toolchains": ["stable"]
        }"#;

        let decoded: WorkerRegistration =
            serde_json::from_str(legacy_json).expect("legacy JSON must deserialize");
        assert_eq!(decoded.node_id, "node-a");
        assert_eq!(decoded.arch, "x86_64");
        assert_eq!(decoded.tags, vec!["x86_64", "clippy"]);
        assert_eq!(decoded.max_slots, 1);
        assert_eq!(decoded.ram_total_mb, 16384);
        assert!(!decoded.btrfs_enabled);
        assert_eq!(decoded.toolchains, vec!["stable"]);
        assert_eq!(decoded.rust_semver, None);

        let default_reg = WorkerRegistration::default();
        assert_eq!(default_reg.rust_semver, None);
    }

    #[test]
    fn worker_registration_fory_schema_evolution_compat() {
        let mut legacy_fory = Fory::builder().xlang(false).build();
        legacy_fory
            .register::<LegacyWorkerRegistration>(FORY_ID_REGISTRATION)
            .expect("register legacy WorkerRegistration");

        let modern_fory = create_fory_runtime().expect("Fory runtime");

        let legacy_reg = LegacyWorkerRegistration {
            node_id: "node-a".to_string(),
            arch: "x86_64".to_string(),
            tags: vec!["x86_64".to_string()],
            max_slots: 1,
            ram_total_mb: 32768,
            btrfs_enabled: true,
            toolchains: vec!["system".to_string()],
        };

        // Old worker -> new controller: missing rust_semver defaults to None
        let legacy_bytes = legacy_fory
            .serialize(&legacy_reg)
            .expect("serialize legacy registration");
        let upgraded: WorkerRegistration = modern_fory
            .deserialize(&legacy_bytes)
            .expect("new controller must deserialize legacy registration");
        assert_eq!(upgraded.node_id, legacy_reg.node_id);
        assert_eq!(upgraded.arch, legacy_reg.arch);
        assert_eq!(upgraded.tags, legacy_reg.tags);
        assert_eq!(upgraded.max_slots, legacy_reg.max_slots);
        assert_eq!(upgraded.ram_total_mb, legacy_reg.ram_total_mb);
        assert_eq!(upgraded.btrfs_enabled, legacy_reg.btrfs_enabled);
        assert_eq!(upgraded.toolchains, legacy_reg.toolchains);
        assert_eq!(upgraded.rust_semver, None);

        // New worker -> old controller: unknown rust_semver is skipped safely
        let modern_reg = sample_registration(Some("1.86.0"));
        let modern_bytes = modern_fory
            .serialize(&modern_reg)
            .expect("serialize modern registration");
        let downgraded: LegacyWorkerRegistration = legacy_fory
            .deserialize(&modern_bytes)
            .expect("old controller must deserialize modern registration");
        assert_eq!(downgraded.node_id, modern_reg.node_id);
        assert_eq!(downgraded.arch, modern_reg.arch);
        assert_eq!(downgraded.tags, modern_reg.tags);
        assert_eq!(downgraded.max_slots, modern_reg.max_slots);
        assert_eq!(downgraded.ram_total_mb, modern_reg.ram_total_mb);
        assert_eq!(downgraded.btrfs_enabled, modern_reg.btrfs_enabled);
        assert_eq!(downgraded.toolchains, modern_reg.toolchains);
    }
}

use crate::error::IpcError;
use fory::{Fory, ForyStruct};

pub const FORY_ID_REGISTRATION: u32 = 1;
pub const FORY_ID_HEARTBEAT: u32 = 2;
pub const FORY_ID_DISPATCH: u32 = 3;
pub const FORY_ID_COMPLETION: u32 = 4;
pub const FORY_ID_CANCEL: u32 = 5;
pub const FORY_ID_WORKER_SHUTDOWN: u32 = 6;
pub const FORY_ID_STAGE_STARTED: u32 = 7;

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct WorkerRegistration {
    pub node_id: String,
    pub arch: String,
    pub tags: Vec<String>,
    pub max_slots: u32,
    pub ram_total_mb: u64,
    pub btrfs_enabled: bool,
    pub toolchains: Vec<String>,
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct WorkerHeartbeat {
    pub node_id: String,
    pub active_slots: u32,
    pub cpu_load_pct: f32,
    pub ram_used_mb: u64,
    pub timestamp_epoch_ms: u64,
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
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
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct StageCompletionReport {
    pub job_id: String,
    pub stage_id: String,
    pub exit_code: i32,
    pub duration_secs: u64,
    pub error_message: Option<String>,
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct StageCancelTask {
    pub job_id: String,
    pub stage_id: String,
    pub reason: String,
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct WorkerShutdown {
    pub node_id: String,
    pub reason: String,
    pub interrupted_stages: Vec<String>,
}

#[derive(ForyStruct, Debug, PartialEq, Clone)]
pub struct StageStarted {
    pub job_id: String,
    pub stage_id: String,
    pub node_id: String,
    pub pid: u32,
    pub started_at_epoch_ms: u64,
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
    Ok(fory)
}

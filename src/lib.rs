pub mod arrow_stream;
pub mod control;
pub mod error;
pub mod framing;

pub use arrow_stream::{
    decode_log_batch, decode_metric_batch, encode_log_batch, encode_metric_batch, log_schema,
    metric_schema, LogEntry, MetricEntry,
};
pub use control::{
    create_fory_runtime, StageCancelTask, StageCompletionReport, StageDispatchTask,
    WorkerHeartbeat, WorkerRegistration, WorkerShutdown, FORY_ID_CANCEL, FORY_ID_COMPLETION,
    FORY_ID_DISPATCH, FORY_ID_HEARTBEAT, FORY_ID_REGISTRATION, FORY_ID_WORKER_SHUTDOWN,
};
pub use error::IpcError;
pub use framing::{
    read_frame, write_frame, IpcHeader, CURRENT_VERSION, FLAG_COMPRESSED, FLAG_END_OF_STREAM,
    HEADER_SIZE, MAGIC, MSG_TYPE_ARROW_LOG, MSG_TYPE_ARROW_METRIC, MSG_TYPE_STAGE_CANCEL,
    MSG_TYPE_STAGE_COMPLETION, MSG_TYPE_STAGE_DISPATCH, MSG_TYPE_WORKER_HEARTBEAT,
    MSG_TYPE_WORKER_REGISTRATION, MSG_TYPE_WORKER_SHUTDOWN,
};

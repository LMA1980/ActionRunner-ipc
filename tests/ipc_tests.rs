use action_runner_ipc::{
    create_fory_runtime, decode_log_batch, decode_metric_batch, encode_log_batch,
    encode_metric_batch, read_frame, write_frame, IpcError, IpcHeader, LogEntry, MetricEntry,
    StageCancelTask, StageCompletionReport, StageDispatchTask, WorkerHeartbeat, WorkerRegistration,
    WorkerShutdown, CURRENT_VERSION, HEADER_SIZE, MAX_FRAME_SIZE, MSG_TYPE_ARROW_LOG,
    MSG_TYPE_ARROW_METRIC, MSG_TYPE_STAGE_CANCEL, MSG_TYPE_STAGE_COMPLETION,
    MSG_TYPE_STAGE_DISPATCH, MSG_TYPE_WORKER_HEARTBEAT, MSG_TYPE_WORKER_REGISTRATION,
    MSG_TYPE_WORKER_SHUTDOWN,
};
use tokio::io::{duplex, AsyncWriteExt};

#[test]
fn test_zerocopy_header_parsing() {
    let header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, 512, 0, 1001);
    let bytes = zerocopy::IntoBytes::as_bytes(&header);
    assert_eq!(bytes.len(), 24);

    let (parsed, rest) = IpcHeader::parse(bytes).expect("Valid header");
    assert_eq!(parsed, header);
    assert!(rest.is_empty());
}

#[tokio::test]
async fn test_fory_control_transmission_over_duplex() {
    let (mut client, mut server) = duplex(4096);
    let fory = create_fory_runtime().expect("Fory runtime");

    // 1. WorkerRegistration
    let reg = WorkerRegistration {
        node_id: "rubicon".to_string(),
        arch: "x86_64".to_string(),
        tags: vec![
            "x86_64".to_string(),
            "clippy".to_string(),
            "heavy".to_string(),
        ],
        max_slots: 4,
        ram_total_mb: 32768,
        btrfs_enabled: true,
        toolchains: vec![
            "system".to_string(),
            "llvm-24".to_string(),
            "gcc-17".to_string(),
        ],
    };

    let serialized_payload = fory.serialize(&reg).expect("Serialization");
    let header = IpcHeader::new(
        MSG_TYPE_WORKER_REGISTRATION,
        serialized_payload.len() as u32,
        0,
        1,
    );

    write_frame(&mut client, &header, &serialized_payload)
        .await
        .expect("write frame");

    let (received_hdr, received_payload) = read_frame(&mut server).await.expect("read frame");
    assert_eq!(received_hdr, header);

    let decoded_reg: WorkerRegistration = fory
        .deserialize(&received_payload)
        .expect("Deserialization");
    assert_eq!(reg, decoded_reg);

    // 2. StageDispatchTask
    let task = StageDispatchTask {
        job_id: "job-101".to_string(),
        stage_id: "clippy-check".to_string(),
        stage_name: "Verify Clippy Linter".to_string(),
        repo_name: "LMA1980/model-runner".to_string(),
        clone_url: "git@github.com:LMA1980/model-runner.git".to_string(),
        commit_sha: "abcd1234ef567890".to_string(),
        command: "cargo clippy --workspace --all-targets -- -D warnings".to_string(),
        working_dir: Some("code".to_string()),
        env_vars: vec![("CARGO_TERM_COLOR".to_string(), "always".to_string())],
        timeout_seconds: 360,
        threads: 4,
        toolchain: Some("system".to_string()),
        exclusive: false,
    };

    let task_bytes = fory.serialize(&task).expect("task serialize");
    let task_hdr = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, task_bytes.len() as u32, 0, 2);
    write_frame(&mut client, &task_hdr, &task_bytes)
        .await
        .expect("write task");
    let (recv_task_hdr, recv_task_bytes) = read_frame(&mut server).await.expect("read task");
    assert_eq!(recv_task_hdr, task_hdr);
    let decoded_task: StageDispatchTask = fory.deserialize(&recv_task_bytes).expect("decode task");
    assert_eq!(task, decoded_task);

    // 3. StageCompletionReport
    let report = StageCompletionReport {
        job_id: "job-101".to_string(),
        stage_id: "clippy-check".to_string(),
        exit_code: 0,
        duration_secs: 42,
        error_message: None,
    };
    let report_bytes = fory.serialize(&report).expect("report serialize");
    let report_hdr = IpcHeader::new(MSG_TYPE_STAGE_COMPLETION, report_bytes.len() as u32, 0, 3);
    write_frame(&mut client, &report_hdr, &report_bytes)
        .await
        .expect("write report");
    let (recv_report_hdr, recv_report_bytes) = read_frame(&mut server).await.expect("read report");
    assert_eq!(recv_report_hdr, report_hdr);
    let decoded_report: StageCompletionReport =
        fory.deserialize(&recv_report_bytes).expect("decode report");
    assert_eq!(report, decoded_report);

    // 4. WorkerHeartbeat
    let hb = WorkerHeartbeat {
        node_id: "rubicon".to_string(),
        active_slots: 2,
        cpu_load_pct: 48.5,
        ram_used_mb: 12400,
        timestamp_epoch_ms: 1730000000000,
    };
    let hb_bytes = fory.serialize(&hb).expect("hb serialize");
    let hb_hdr = IpcHeader::new(MSG_TYPE_WORKER_HEARTBEAT, hb_bytes.len() as u32, 0, 4);
    write_frame(&mut client, &hb_hdr, &hb_bytes)
        .await
        .expect("write hb");
    let (recv_hb_hdr, recv_hb_bytes) = read_frame(&mut server).await.expect("read hb");
    assert_eq!(recv_hb_hdr, hb_hdr);
    let decoded_hb: WorkerHeartbeat = fory.deserialize(&recv_hb_bytes).expect("decode hb");
    assert_eq!(hb, decoded_hb);

    // 5. StageCancelTask
    let cancel = StageCancelTask {
        job_id: "job-101".to_string(),
        stage_id: "clippy-check".to_string(),
        reason: "Newer commit pushed (superseded)".to_string(),
    };
    let cancel_bytes = fory.serialize(&cancel).expect("cancel serialize");
    let cancel_hdr = IpcHeader::new(MSG_TYPE_STAGE_CANCEL, cancel_bytes.len() as u32, 0, 5);
    write_frame(&mut client, &cancel_hdr, &cancel_bytes)
        .await
        .expect("write cancel");
    let (recv_cancel_hdr, recv_cancel_bytes) = read_frame(&mut server).await.expect("read cancel");
    assert_eq!(recv_cancel_hdr, cancel_hdr);
    let decoded_cancel: StageCancelTask =
        fory.deserialize(&recv_cancel_bytes).expect("decode cancel");
    assert_eq!(cancel, decoded_cancel);
}

#[tokio::test]
async fn test_arrow_log_batch_streaming_over_duplex() {
    let (mut client, mut server) = duplex(8192);

    let log_entries = vec![
        LogEntry {
            timestamp_us: 1730000000000000,
            job_id: "job-101".to_string(),
            stage_id: "clippy-check".to_string(),
            node_id: "rubicon".to_string(),
            stream: 1,
            line_no: 1,
            content: "Running cargo clippy --workspace --all-targets -- -D warnings".to_string(),
        },
        LogEntry {
            timestamp_us: 1730000001000000,
            job_id: "job-101".to_string(),
            stage_id: "clippy-check".to_string(),
            node_id: "rubicon".to_string(),
            stream: 1,
            line_no: 2,
            content: "Checking k3-core v0.1.0...".to_string(),
        },
        LogEntry {
            timestamp_us: 1730000002000000,
            job_id: "job-101".to_string(),
            stage_id: "clippy-check".to_string(),
            node_id: "rubicon".to_string(),
            stream: 2,
            line_no: 3,
            content: "warning: unused variable in test module".to_string(),
        },
    ];

    let ipc_bytes = encode_log_batch(&log_entries).expect("Arrow encoding");
    let header = IpcHeader::new(MSG_TYPE_ARROW_LOG, ipc_bytes.len() as u32, 0, 42);

    write_frame(&mut client, &header, &ipc_bytes)
        .await
        .expect("write log frame");

    let (recv_hdr, recv_payload) = read_frame(&mut server).await.expect("read log frame");
    assert_eq!(recv_hdr, header);

    let decoded_entries = decode_log_batch(&recv_payload).expect("Arrow decoding");
    assert_eq!(log_entries, decoded_entries);
}

#[tokio::test]
async fn test_arrow_metrics_streaming_over_duplex() {
    let (mut client, mut server) = duplex(8192);

    let metric_entries = vec![
        MetricEntry {
            timestamp_us: 1730000000000000,
            job_id: "job-101".to_string(),
            stage_id: "native-k3-verification".to_string(),
            submodule: "k3-core".to_string(),
            tests_passed: 48,
            tests_total: 48,
            coverage_pct: Some(89.5),
            clippy_warnings: 0,
            advisories_count: 0,
            bench_ns_per_iter: Some(12.4),
        },
        MetricEntry {
            timestamp_us: 1730000001000000,
            job_id: "job-101".to_string(),
            stage_id: "native-k3-verification".to_string(),
            submodule: "k3-engine".to_string(),
            tests_passed: 112,
            tests_total: 112,
            coverage_pct: Some(92.1),
            clippy_warnings: 0,
            advisories_count: 0,
            bench_ns_per_iter: Some(48.0),
        },
    ];

    let ipc_bytes = encode_metric_batch(&metric_entries).expect("Arrow metric encoding");
    let header = IpcHeader::new(MSG_TYPE_ARROW_METRIC, ipc_bytes.len() as u32, 0, 43);

    write_frame(&mut client, &header, &ipc_bytes)
        .await
        .expect("write metric frame");

    let (recv_hdr, recv_payload) = read_frame(&mut server).await.expect("read metric frame");
    assert_eq!(recv_hdr, header);

    let decoded_entries = decode_metric_batch(&recv_payload).expect("Arrow metric decoding");
    assert_eq!(metric_entries, decoded_entries);
}

#[tokio::test]
async fn test_worker_shutdown_transmission_over_duplex() {
    let (mut client, mut server) = duplex(4096);
    let fory = create_fory_runtime().expect("Fory runtime");

    let shutdown = WorkerShutdown {
        node_id: "pey-wail-deu".to_string(),
        reason: "Daemon service restarting (SIGTERM)".to_string(),
        interrupted_stages: vec!["miri-check".to_string(), "bench-run".to_string()],
    };

    let serialized_payload = fory.serialize(&shutdown).expect("Serialization");
    let header = IpcHeader::new(
        MSG_TYPE_WORKER_SHUTDOWN,
        serialized_payload.len() as u32,
        0,
        99,
    );

    write_frame(&mut client, &header, &serialized_payload)
        .await
        .expect("write frame");

    let (received_hdr, received_payload) = read_frame(&mut server).await.expect("read frame");
    assert_eq!(received_hdr, header);

    let decoded_shutdown: WorkerShutdown = fory
        .deserialize(&received_payload)
        .expect("Deserialization");
    assert_eq!(decoded_shutdown, shutdown);
}

#[tokio::test]
async fn test_stage_started_transmission_over_duplex() {
    let (mut client, mut server) = duplex(4096);
    let fory = create_fory_runtime().expect("Fory runtime");

    let started = action_runner_ipc::StageStarted {
        job_id: "job-100".to_string(),
        stage_id: "miri-check".to_string(),
        node_id: "pey-wail-deu".to_string(),
        pid: 48210,
        started_at_epoch_ms: 1729790000000,
    };

    let serialized_payload = fory.serialize(&started).expect("Serialization");
    let header = IpcHeader::new(
        action_runner_ipc::MSG_TYPE_STAGE_STARTED,
        serialized_payload.len() as u32,
        0,
        100,
    );

    write_frame(&mut client, &header, &serialized_payload)
        .await
        .expect("write frame");

    let (received_hdr, received_payload) = read_frame(&mut server).await.expect("read frame");
    assert_eq!(received_hdr, header);

    let decoded_started: action_runner_ipc::StageStarted = fory
        .deserialize(&received_payload)
        .expect("Deserialization");
    assert_eq!(decoded_started, started);
}

#[test]
fn test_fory_stage_dispatch_task_exclusive_true() {
    let fory = create_fory_runtime().expect("Fory runtime");
    let task = StageDispatchTask {
        job_id: "job-exclusive-101".to_string(),
        stage_id: "bench-run".to_string(),
        stage_name: "Hardware Micro-Benchmark".to_string(),
        repo_name: "LMA1980/model-runner".to_string(),
        clone_url: "git@github.com:LMA1980/model-runner.git".to_string(),
        commit_sha: "abcd1234ef567890".to_string(),
        command: "cargo bench -p k3-kernels-rvv".to_string(),
        working_dir: Some("code".to_string()),
        env_vars: vec![("BENCH_MODE".to_string(), "exclusive".to_string())],
        timeout_seconds: 1800,
        threads: 16,
        toolchain: Some("nightly".to_string()),
        exclusive: true,
    };

    let serialized = fory
        .serialize(&task)
        .expect("serialize StageDispatchTask (exclusive: true)");
    assert!(
        !serialized.is_empty(),
        "Serialized bytes should not be empty"
    );

    let decoded: StageDispatchTask = fory
        .deserialize(&serialized)
        .expect("deserialize StageDispatchTask (exclusive: true)");
    assert_eq!(task, decoded);
    assert!(decoded.exclusive, "Decoded exclusive flag must be true");
}

#[test]
fn test_fory_stage_dispatch_task_exclusive_false() {
    let fory = create_fory_runtime().expect("Fory runtime");
    let task = StageDispatchTask {
        job_id: "job-shared-202".to_string(),
        stage_id: "cargo-check".to_string(),
        stage_name: "Workspace Check".to_string(),
        repo_name: "LMA1980/model-runner".to_string(),
        clone_url: "git@github.com:LMA1980/model-runner.git".to_string(),
        commit_sha: "1234567890abcdef".to_string(),
        command: "cargo check --workspace".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 300,
        threads: 4,
        toolchain: None,
        exclusive: false,
    };

    let serialized = fory
        .serialize(&task)
        .expect("serialize StageDispatchTask (exclusive: false)");
    assert!(
        !serialized.is_empty(),
        "Serialized bytes should not be empty"
    );

    let decoded: StageDispatchTask = fory
        .deserialize(&serialized)
        .expect("deserialize StageDispatchTask (exclusive: false)");
    assert_eq!(task, decoded);
    assert!(!decoded.exclusive, "Decoded exclusive flag must be false");
}

#[tokio::test]
async fn test_stage_dispatch_task_duplex_transmission_with_exclusivity() {
    let (mut client, mut server) = duplex(4096);
    let fory = create_fory_runtime().expect("Fory runtime");

    // 1. Send exclusive = true task
    let exclusive_task = StageDispatchTask {
        job_id: "job-bench-01".to_string(),
        stage_id: "bench-run".to_string(),
        stage_name: "Hardware Micro-Benchmark".to_string(),
        repo_name: "LMA1980/model-runner".to_string(),
        clone_url: "git@github.com:LMA1980/model-runner.git".to_string(),
        commit_sha: "commit-bench-1".to_string(),
        command: "cargo bench".to_string(),
        working_dir: Some("bench".to_string()),
        env_vars: vec![],
        timeout_seconds: 600,
        threads: 8,
        toolchain: Some("system".to_string()),
        exclusive: true,
    };
    let payload_ex = fory
        .serialize(&exclusive_task)
        .expect("serialize exclusive task");
    let hdr_ex = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, payload_ex.len() as u32, 0, 101);
    write_frame(&mut client, &hdr_ex, &payload_ex)
        .await
        .expect("write exclusive frame");
    let (recv_hdr_ex, recv_bytes_ex) = read_frame(&mut server).await.expect("read exclusive frame");
    assert_eq!(recv_hdr_ex, hdr_ex);
    let decoded_ex: StageDispatchTask = fory
        .deserialize(&recv_bytes_ex)
        .expect("deserialize exclusive task");
    assert_eq!(exclusive_task, decoded_ex);
    assert!(decoded_ex.exclusive);

    // 2. Send exclusive = false task
    let shared_task = StageDispatchTask {
        job_id: "job-clippy-02".to_string(),
        stage_id: "clippy-check".to_string(),
        stage_name: "Clippy Check".to_string(),
        repo_name: "LMA1980/model-runner".to_string(),
        clone_url: "git@github.com:LMA1980/model-runner.git".to_string(),
        commit_sha: "commit-clippy-2".to_string(),
        command: "cargo clippy".to_string(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 300,
        threads: 2,
        toolchain: None,
        exclusive: false,
    };
    let payload_sh = fory.serialize(&shared_task).expect("serialize shared task");
    let hdr_sh = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, payload_sh.len() as u32, 0, 102);
    write_frame(&mut client, &hdr_sh, &payload_sh)
        .await
        .expect("write shared frame");
    let (recv_hdr_sh, recv_bytes_sh) = read_frame(&mut server).await.expect("read shared frame");
    assert_eq!(recv_hdr_sh, hdr_sh);
    let decoded_sh: StageDispatchTask = fory
        .deserialize(&recv_bytes_sh)
        .expect("deserialize shared task");
    assert_eq!(shared_task, decoded_sh);
    assert!(!decoded_sh.exclusive);
}

#[tokio::test]
async fn test_read_frame_rejects_oversized_payload() {
    let (mut client, mut server) = duplex(1024);

    let oversized_len = MAX_FRAME_SIZE + 1;
    let header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, oversized_len as u32, 0, 1);

    client
        .write_all(zerocopy::IntoBytes::as_bytes(&header))
        .await
        .expect("write header");

    let err = read_frame(&mut server)
        .await
        .expect_err("read_frame must reject oversized payload");

    match err {
        IpcError::FrameTooLarge { max, actual } => {
            assert_eq!(max, MAX_FRAME_SIZE);
            assert_eq!(actual, oversized_len);
        }
        other => panic!("Expected IpcError::FrameTooLarge, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_read_frame_rejects_u32_max_payload() {
    let (mut client, mut server) = duplex(1024);

    let header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, u32::MAX, 0, 999);

    client
        .write_all(zerocopy::IntoBytes::as_bytes(&header))
        .await
        .expect("write header");

    let err = read_frame(&mut server)
        .await
        .expect_err("read_frame must reject u32::MAX payload");

    match err {
        IpcError::FrameTooLarge { max, actual } => {
            assert_eq!(max, MAX_FRAME_SIZE);
            assert_eq!(actual, u32::MAX as usize);
        }
        other => panic!("Expected IpcError::FrameTooLarge, got: {:?}", other),
    }
}

#[test]
fn test_header_parse_rejects_unsupported_version_future() {
    let mut header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, 64, 0, 1);
    header.version = 2;
    let bytes = zerocopy::IntoBytes::as_bytes(&header);

    let err = IpcHeader::parse(bytes).expect_err("Should reject version 2");
    match err {
        IpcError::UnsupportedVersion { expected, actual } => {
            assert_eq!(expected, CURRENT_VERSION);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected IpcError::UnsupportedVersion, got: {:?}", other),
    }
}

#[test]
fn test_header_parse_rejects_unsupported_version_zero() {
    let mut header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, 64, 0, 1);
    header.version = 0;
    let bytes = zerocopy::IntoBytes::as_bytes(&header);

    let err = IpcHeader::parse(bytes).expect_err("Should reject version 0");
    match err {
        IpcError::UnsupportedVersion { expected, actual } => {
            assert_eq!(expected, CURRENT_VERSION);
            assert_eq!(actual, 0);
        }
        other => panic!("Expected IpcError::UnsupportedVersion, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_read_frame_rejects_unsupported_version_over_duplex() {
    let (mut client, mut server) = duplex(1024);

    let mut header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, 32, 0, 1);
    header.version = 99;

    client
        .write_all(zerocopy::IntoBytes::as_bytes(&header))
        .await
        .expect("write header");

    let err = read_frame(&mut server)
        .await
        .expect_err("read_frame must reject unsupported version");

    match err {
        IpcError::UnsupportedVersion { expected, actual } => {
            assert_eq!(expected, CURRENT_VERSION);
            assert_eq!(actual, 99);
        }
        other => panic!("Expected IpcError::UnsupportedVersion, got: {:?}", other),
    }
}

#[test]
fn test_header_parse_rejects_invalid_magic() {
    let mut header = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, 64, 0, 1);
    header.magic = *b"NOPE";
    let bytes = zerocopy::IntoBytes::as_bytes(&header);

    let err = IpcHeader::parse(bytes).expect_err("Should reject invalid magic");
    assert!(matches!(err, IpcError::InvalidMagic));
}

#[test]
fn test_header_parse_rejects_buffer_too_small() {
    let bytes = [0u8; 12];
    let err = IpcHeader::parse(&bytes).expect_err("Should reject truncated buffer");
    match err {
        IpcError::BufferTooSmall { expected, actual } => {
            assert_eq!(expected, HEADER_SIZE);
            assert_eq!(actual, 12);
        }
        other => panic!("Expected IpcError::BufferTooSmall, got: {:?}", other),
    }
}

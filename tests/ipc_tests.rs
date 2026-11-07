use mu_ci_ipc::{
    bench_runs_schema, create_fory_runtime, decode_bench_runs_batch, decode_log_batch,
    decode_metric_batch, decode_test_runs_batch, encode_bench_runs_batch, encode_log_batch,
    encode_metric_batch, encode_test_runs_batch, read_frame, test_runs_schema, write_frame,
    BenchmarkRunRecord, HostPrivilegeCeilingRequest, HostPrivilegeCeilingResponse, IpcError,
    IpcHeader, LogEntry, MetricEntry, NetworkEgressMode, SandboxBackendKind, SandboxConstraintSpec,
    SccacheMode, SecurityGrantDenied, StageCancelTask, StageCompletionReport, StageDispatchTask,
    StageStarted, TestCaseRunRecord, TypedTaskInvocation, WorkerHeartbeat, WorkerRegistration,
    WorkerShutdown, CURRENT_VERSION, HEADER_SIZE, MAX_FRAME_SIZE, MSG_TYPE_ARROW_BENCH_RUNS,
    MSG_TYPE_ARROW_LOG, MSG_TYPE_ARROW_METRIC, MSG_TYPE_ARROW_TEST_RUNS, MSG_TYPE_CEILING_REQ,
    MSG_TYPE_CEILING_RESP, MSG_TYPE_STAGE_CANCEL, MSG_TYPE_STAGE_COMPLETION,
    MSG_TYPE_STAGE_DISPATCH, MSG_TYPE_STAGE_STARTED, MSG_TYPE_WORKER_HEARTBEAT,
    MSG_TYPE_WORKER_REGISTRATION, MSG_TYPE_WORKER_SHUTDOWN,
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
        node_id: "node-a".to_string(),
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
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
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
        security_grant_denied: None,
        traceparent: None,
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
        node_id: "node-a".to_string(),
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
            node_id: "node-a".to_string(),
            stream: 1,
            line_no: 1,
            content: "Running cargo clippy --workspace --all-targets -- -D warnings".to_string(),
        },
        LogEntry {
            timestamp_us: 1730000001000000,
            job_id: "job-101".to_string(),
            stage_id: "clippy-check".to_string(),
            node_id: "node-a".to_string(),
            stream: 1,
            line_no: 2,
            content: "Checking k3-core v0.1.0...".to_string(),
        },
        LogEntry {
            timestamp_us: 1730000002000000,
            job_id: "job-101".to_string(),
            stage_id: "clippy-check".to_string(),
            node_id: "node-a".to_string(),
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
        node_id: "worker-riscv64".to_string(),
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

    let started = mu_ci_ipc::StageStarted {
        job_id: "job-100".to_string(),
        stage_id: "miri-check".to_string(),
        node_id: "worker-riscv64".to_string(),
        pid: 48210,
        started_at_epoch_ms: 1729790000000,
        traceparent: None,
    };

    let serialized_payload = fory.serialize(&started).expect("Serialization");
    let header = IpcHeader::new(
        mu_ci_ipc::MSG_TYPE_STAGE_STARTED,
        serialized_payload.len() as u32,
        0,
        100,
    );

    write_frame(&mut client, &header, &serialized_payload)
        .await
        .expect("write frame");

    let (received_hdr, received_payload) = read_frame(&mut server).await.expect("read frame");
    assert_eq!(received_hdr, header);

    let decoded_started: mu_ci_ipc::StageStarted = fory
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
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
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
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
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
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
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
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
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

#[tokio::test]
async fn test_ceiling_handshake_and_sandbox_types_over_duplex() {
    let (mut client, mut server) = duplex(8192);
    let fory = create_fory_runtime().expect("Fory runtime");

    let req = HostPrivilegeCeilingRequest {
        request_id: "req-ceiling-001".to_string(),
        controller_version: CURRENT_VERSION,
    };
    let req_bytes = fory.serialize(&req).expect("serialize ceiling req");
    let req_hdr = IpcHeader::new(MSG_TYPE_CEILING_REQ, req_bytes.len() as u32, 0, 201);
    write_frame(&mut client, &req_hdr, &req_bytes)
        .await
        .expect("write ceiling req");
    let (recv_req_hdr, recv_req_bytes) = read_frame(&mut server).await.expect("read ceiling req");
    assert_eq!(recv_req_hdr, req_hdr);
    let decoded_req: HostPrivilegeCeilingRequest = fory
        .deserialize(&recv_req_bytes)
        .expect("decode ceiling req");
    assert_eq!(req, decoded_req);

    let ceiling = SandboxConstraintSpec {
        profile_name: "host-operator-ceiling".to_string(),
        backend: SandboxBackendKind::Hypervisor,
        read_paths: vec!["/usr".to_string(), "/lib".to_string()],
        write_paths: vec!["/var/lib/action-runner/work".to_string()],
        exec_paths: vec!["/usr/bin".to_string()],
        network_mode: NetworkEgressMode::RestrictedCidrs(vec![
            "192.0.2.0/24".to_string(),
            "198.51.100.0/24".to_string(),
        ]),
        memlock_limit_kb: 29_360_128,
        max_pids: 512,
        require_set_ai_thread: true,
    };

    let resp = HostPrivilegeCeilingResponse {
        request_id: "req-ceiling-001".to_string(),
        node_id: "worker-riscv64".to_string(),
        ceiling: ceiling.clone(),
        kvm_available: true,
        spacemit_ai_thread_available: true,
    };
    let resp_bytes = fory.serialize(&resp).expect("serialize ceiling resp");
    let resp_hdr = IpcHeader::new(MSG_TYPE_CEILING_RESP, resp_bytes.len() as u32, 0, 202);
    write_frame(&mut server, &resp_hdr, &resp_bytes)
        .await
        .expect("write ceiling resp");
    let (recv_resp_hdr, recv_resp_bytes) =
        read_frame(&mut client).await.expect("read ceiling resp");
    assert_eq!(recv_resp_hdr, resp_hdr);
    let decoded_resp: HostPrivilegeCeilingResponse = fory
        .deserialize(&recv_resp_bytes)
        .expect("decode ceiling resp");
    assert_eq!(resp, decoded_resp);

    // Verify StageDispatchTask with TypedTaskInvocation and StageCompletionReport with SecurityGrantDenied
    let dispatch = StageDispatchTask {
        job_id: "job-sec-301".to_string(),
        stage_id: "sandboxed-stage".to_string(),
        stage_name: "Sandboxed Build".to_string(),
        repo_name: "LMA1980/ActionRunner-rs".to_string(),
        clone_url: "https://github.com/LMA1980/ActionRunner-rs.git".to_string(),
        commit_sha: "deadbeef12345678".to_string(),
        command: String::new(),
        working_dir: Some(".".to_string()),
        env_vars: vec![("RUST_BACKTRACE".to_string(), "1".to_string())],
        timeout_seconds: 600,
        threads: 6,
        toolchain: Some("stable".to_string()),
        exclusive: true,
        max_concurrent_per_node: 1,
        tasks: vec![TypedTaskInvocation {
            task_id: "task-1".to_string(),
            tool_name: "cargo".to_string(),
            args: vec!["test".to_string(), "--workspace".to_string()],
            working_dir: None,
            env_vars: vec![],
            sandbox: SandboxConstraintSpec {
                profile_name: "default-offline".to_string(),
                backend: SandboxBackendKind::Kernel,
                read_paths: vec!["/usr".to_string()],
                write_paths: vec!["/tmp".to_string()],
                exec_paths: vec!["/usr/bin".to_string()],
                network_mode: NetworkEgressMode::Offline,
                memlock_limit_kb: 65536,
                max_pids: 64,
                require_set_ai_thread: false,
            },
            sccache_mode: SccacheMode::default(),
        }],
        sandbox_constraints: ceiling,
        traceparent: None,
        sccache_mode: SccacheMode::default(),
    };
    let dispatch_bytes = fory
        .serialize(&dispatch)
        .expect("serialize typed dispatch task");
    let dispatch_hdr = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, dispatch_bytes.len() as u32, 0, 203);
    write_frame(&mut client, &dispatch_hdr, &dispatch_bytes)
        .await
        .expect("write dispatch frame");
    let (recv_dispatch_hdr, recv_dispatch_bytes) =
        read_frame(&mut server).await.expect("read dispatch frame");
    assert_eq!(recv_dispatch_hdr, dispatch_hdr);
    let decoded_dispatch: StageDispatchTask = fory
        .deserialize(&recv_dispatch_bytes)
        .expect("deserialize typed dispatch task");
    assert_eq!(dispatch, decoded_dispatch);

    let denied_report = StageCompletionReport {
        job_id: "job-sec-301".to_string(),
        stage_id: "sandboxed-stage".to_string(),
        exit_code: 126,
        duration_secs: 1,
        error_message: Some("Security grant denied by monotonic attenuation check".to_string()),
        security_grant_denied: Some(SecurityGrantDenied {
            job_id: "job-sec-301".to_string(),
            stage_id: "sandboxed-stage".to_string(),
            violated_layer: "workflow_override".to_string(),
            offending_field: "memlock_limit_kb".to_string(),
            requested_value: "67108864".to_string(),
            ceiling_value: "29360128".to_string(),
        }),
        traceparent: None,
    };
    let report_bytes = fory
        .serialize(&denied_report)
        .expect("serialize denied completion report");
    let report_hdr = IpcHeader::new(MSG_TYPE_STAGE_COMPLETION, report_bytes.len() as u32, 0, 204);
    write_frame(&mut server, &report_hdr, &report_bytes)
        .await
        .expect("write denied report frame");
    let (recv_report_hdr, recv_report_bytes) = read_frame(&mut client)
        .await
        .expect("read denied report frame");
    assert_eq!(recv_report_hdr, report_hdr);
    let decoded_report: StageCompletionReport = fory
        .deserialize(&recv_report_bytes)
        .expect("deserialize denied completion report");
    assert_eq!(denied_report, decoded_report);
}

#[test]
fn test_stage_dispatch_task_declarative_3tier_modes_fory_and_serde_json_roundtrip() {
    let fory = create_fory_runtime().expect("Fory runtime");

    // Mode A: SandboxBackendKind::Kernel with NetworkEgressMode::Offline
    let task_kernel_offline = StageDispatchTask {
        job_id: "job-3tier-kernel".to_string(),
        stage_id: "cargo-test-kernel".to_string(),
        stage_name: "Kernel Sandboxed Offline Unit Tests".to_string(),
        repo_name: "LMA1980/ActionRunner-rs".to_string(),
        clone_url: "https://github.com/LMA1980/ActionRunner-rs.git".to_string(),
        commit_sha: "aaaaaaaa111122223333".to_string(),
        command: String::new(),
        working_dir: Some("runner".to_string()),
        env_vars: vec![("RUST_LOG".to_string(), "info".to_string())],
        timeout_seconds: 400,
        threads: 4,
        toolchain: Some("stable".to_string()),
        exclusive: true,
        max_concurrent_per_node: 1,
        tasks: vec![TypedTaskInvocation {
            task_id: "task-offline-1".to_string(),
            tool_name: "cargo".to_string(),
            args: vec!["test".to_string(), "--workspace".to_string()],
            working_dir: Some("code".to_string()),
            env_vars: vec![("OFFLINE_BUILD".to_string(), "1".to_string())],
            sandbox: SandboxConstraintSpec {
                profile_name: "kernel-offline".to_string(),
                backend: SandboxBackendKind::Kernel,
                read_paths: vec!["/usr".to_string(), "/lib".to_string()],
                write_paths: vec!["/var/lib/action-runner/work".to_string()],
                exec_paths: vec!["/usr/bin".to_string()],
                network_mode: NetworkEgressMode::Offline,
                memlock_limit_kb: 65536,
                max_pids: 128,
                require_set_ai_thread: false,
            },
            sccache_mode: SccacheMode::default(),
        }],
        sandbox_constraints: SandboxConstraintSpec {
            profile_name: "node-ceiling-kernel".to_string(),
            backend: SandboxBackendKind::Kernel,
            read_paths: vec!["/usr".to_string(), "/lib".to_string()],
            write_paths: vec!["/var/lib/action-runner/work".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: NetworkEgressMode::Offline,
            memlock_limit_kb: 131072,
            max_pids: 256,
            require_set_ai_thread: false,
        },
        traceparent: None,
        sccache_mode: SccacheMode::default(),
    };

    // Apache Fory binary roundtrip for Mode A
    let fory_bytes_a = fory
        .serialize(&task_kernel_offline)
        .expect("fory serialize Mode A");
    let fory_decoded_a: StageDispatchTask = fory
        .deserialize(&fory_bytes_a)
        .expect("fory deserialize Mode A");
    assert_eq!(task_kernel_offline, fory_decoded_a);
    assert_eq!(
        fory_decoded_a.sandbox_constraints.backend,
        SandboxBackendKind::Kernel
    );
    assert_eq!(
        fory_decoded_a.sandbox_constraints.network_mode,
        NetworkEgressMode::Offline
    );

    // serde_json roundtrip for Mode A
    let json_str_a =
        serde_json::to_string(&task_kernel_offline).expect("serde_json serialize Mode A");
    let json_decoded_a: StageDispatchTask =
        serde_json::from_str(&json_str_a).expect("serde_json deserialize Mode A");
    assert_eq!(task_kernel_offline, json_decoded_a);

    // Mode B: SandboxBackendKind::Hypervisor with NetworkEgressMode::RestrictedCidrs
    let task_hypervisor_restricted = StageDispatchTask {
        job_id: "job-3tier-hypervisor".to_string(),
        stage_id: "network-fetch-stage".to_string(),
        stage_name: "Hypervisor Sandboxed Egress Stage".to_string(),
        repo_name: "LMA1980/ActionRunner-rs".to_string(),
        clone_url: "https://github.com/LMA1980/ActionRunner-rs.git".to_string(),
        commit_sha: "bbbbbbbb444455556666".to_string(),
        command: String::new(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 1200,
        threads: 8,
        toolchain: Some("nightly".to_string()),
        exclusive: true,
        max_concurrent_per_node: 1,
        tasks: vec![TypedTaskInvocation {
            task_id: "task-restricted-1".to_string(),
            tool_name: "git".to_string(),
            args: vec!["fetch".to_string(), "--all".to_string()],
            working_dir: None,
            env_vars: vec![],
            sandbox: SandboxConstraintSpec {
                profile_name: "hypervisor-restricted".to_string(),
                backend: SandboxBackendKind::Hypervisor,
                read_paths: vec!["/etc".to_string(), "/usr".to_string()],
                write_paths: vec!["/var/lib/action-runner/repo".to_string()],
                exec_paths: vec!["/usr/bin/git".to_string()],
                network_mode: NetworkEgressMode::RestrictedCidrs(vec![
                    "192.0.2.0/24".to_string(),
                    "198.51.100.0/24".to_string(),
                ]),
                memlock_limit_kb: 29_360_128,
                max_pids: 512,
                require_set_ai_thread: true,
            },
            sccache_mode: SccacheMode::Mandatory,
        }],
        sandbox_constraints: SandboxConstraintSpec {
            profile_name: "node-ceiling-hypervisor".to_string(),
            backend: SandboxBackendKind::Hypervisor,
            read_paths: vec!["/etc".to_string(), "/usr".to_string()],
            write_paths: vec!["/var/lib/action-runner/repo".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: NetworkEgressMode::RestrictedCidrs(vec![
                "192.0.2.0/24".to_string(),
                "198.51.100.0/24".to_string(),
            ]),
            memlock_limit_kb: 29_360_128,
            max_pids: 1024,
            require_set_ai_thread: true,
        },
        traceparent: None,
        sccache_mode: SccacheMode::default(),
    };

    // Apache Fory binary roundtrip for Mode B
    let fory_bytes_b = fory
        .serialize(&task_hypervisor_restricted)
        .expect("fory serialize Mode B");
    let fory_decoded_b: StageDispatchTask = fory
        .deserialize(&fory_bytes_b)
        .expect("fory deserialize Mode B");
    assert_eq!(task_hypervisor_restricted, fory_decoded_b);
    assert_eq!(
        fory_decoded_b.sandbox_constraints.backend,
        SandboxBackendKind::Hypervisor
    );
    assert_eq!(
        fory_decoded_b.sandbox_constraints.network_mode,
        NetworkEgressMode::RestrictedCidrs(vec![
            "192.0.2.0/24".to_string(),
            "198.51.100.0/24".to_string(),
        ])
    );

    // serde_json roundtrip for Mode B
    let json_str_b =
        serde_json::to_string(&task_hypervisor_restricted).expect("serde_json serialize Mode B");
    let json_decoded_b: StageDispatchTask =
        serde_json::from_str(&json_str_b).expect("serde_json deserialize Mode B");
    assert_eq!(task_hypervisor_restricted, json_decoded_b);
}

#[test]
fn test_stage_dispatch_task_legacy_fallback_fory_and_serde_json_roundtrip() {
    let fory = create_fory_runtime().expect("Fory runtime");

    // Phase 1-2 legacy fallback mode: tasks is empty, command is provided
    let legacy_task = StageDispatchTask {
        job_id: "job-legacy-999".to_string(),
        stage_id: "legacy-test-stage".to_string(),
        stage_name: "Phase 1-2 Legacy Fallback Execution".to_string(),
        repo_name: "LMA1980/ActionRunner-rs".to_string(),
        clone_url: "https://github.com/LMA1980/ActionRunner-rs.git".to_string(),
        commit_sha: "cccccccc777788889999".to_string(),
        command: "cargo test --workspace".to_string(),
        working_dir: None,
        env_vars: vec![("CI".to_string(), "true".to_string())],
        timeout_seconds: 300,
        threads: 2,
        toolchain: None,
        exclusive: false,
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: None,
        sccache_mode: SccacheMode::default(),
    };

    assert!(
        legacy_task.tasks.is_empty(),
        "Legacy fallback mode tasks must be empty"
    );
    assert_eq!(legacy_task.command, "cargo test --workspace");

    // Apache Fory binary roundtrip
    let fory_bytes = fory
        .serialize(&legacy_task)
        .expect("fory serialize legacy task");
    let fory_decoded: StageDispatchTask = fory
        .deserialize(&fory_bytes)
        .expect("fory deserialize legacy task");
    assert_eq!(legacy_task, fory_decoded);
    assert!(fory_decoded.tasks.is_empty());
    assert_eq!(fory_decoded.command, "cargo test --workspace");

    // serde_json roundtrip
    let json_str = serde_json::to_string(&legacy_task).expect("serde_json serialize legacy task");
    let json_decoded: StageDispatchTask =
        serde_json::from_str(&json_str).expect("serde_json deserialize legacy task");
    assert_eq!(legacy_task, json_decoded);
    assert!(json_decoded.tasks.is_empty());
    assert_eq!(json_decoded.command, "cargo test --workspace");
}

#[tokio::test]
async fn test_host_privilege_ceiling_serde_json_and_duplex_frame_roundtrip() {
    let (mut client, mut server) = duplex(8192);
    let fory = create_fory_runtime().expect("Fory runtime");

    // 1. HostPrivilegeCeilingRequest
    let req = HostPrivilegeCeilingRequest {
        request_id: "req-priv-ceiling-001".to_string(),
        controller_version: CURRENT_VERSION,
    };

    // serde_json roundtrip for request
    let req_json = serde_json::to_string(&req).expect("serde_json serialize ceiling req");
    let req_json_decoded: HostPrivilegeCeilingRequest =
        serde_json::from_str(&req_json).expect("serde_json deserialize ceiling req");
    assert_eq!(req, req_json_decoded);

    // Frame-level roundtrip for request
    let req_fory_bytes = fory.serialize(&req).expect("fory serialize ceiling req");
    let req_hdr = IpcHeader::new(MSG_TYPE_CEILING_REQ, req_fory_bytes.len() as u32, 0, 301);
    write_frame(&mut client, &req_hdr, &req_fory_bytes)
        .await
        .expect("write frame ceiling req");
    let (recv_req_hdr, recv_req_bytes) = read_frame(&mut server)
        .await
        .expect("read frame ceiling req");
    assert_eq!(recv_req_hdr, req_hdr);
    let decoded_req: HostPrivilegeCeilingRequest = fory
        .deserialize(&recv_req_bytes)
        .expect("fory decode ceiling req");
    assert_eq!(req, decoded_req);

    // 2. HostPrivilegeCeilingResponse
    let resp = HostPrivilegeCeilingResponse {
        request_id: "req-priv-ceiling-001".to_string(),
        node_id: "worker-riscv64".to_string(),
        ceiling: SandboxConstraintSpec {
            profile_name: "operator-host-ceiling".to_string(),
            backend: SandboxBackendKind::Hypervisor,
            read_paths: vec!["/usr".to_string(), "/lib".to_string(), "/etc".to_string()],
            write_paths: vec!["/var/lib/action-runner".to_string()],
            exec_paths: vec!["/usr/bin".to_string()],
            network_mode: NetworkEgressMode::RestrictedCidrs(vec![
                "192.0.2.0/24".to_string(),
                "198.51.100.0/24".to_string(),
            ]),
            memlock_limit_kb: 29_360_128,
            max_pids: 1024,
            require_set_ai_thread: true,
        },
        kvm_available: true,
        spacemit_ai_thread_available: true,
    };

    // serde_json roundtrip for response
    let resp_json = serde_json::to_string(&resp).expect("serde_json serialize ceiling resp");
    let resp_json_decoded: HostPrivilegeCeilingResponse =
        serde_json::from_str(&resp_json).expect("serde_json deserialize ceiling resp");
    assert_eq!(resp, resp_json_decoded);

    // Frame-level roundtrip for response
    let resp_fory_bytes = fory.serialize(&resp).expect("fory serialize ceiling resp");
    let resp_hdr = IpcHeader::new(MSG_TYPE_CEILING_RESP, resp_fory_bytes.len() as u32, 0, 302);
    write_frame(&mut server, &resp_hdr, &resp_fory_bytes)
        .await
        .expect("write frame ceiling resp");
    let (recv_resp_hdr, recv_resp_bytes) = read_frame(&mut client)
        .await
        .expect("read frame ceiling resp");
    assert_eq!(recv_resp_hdr, resp_hdr);
    let decoded_resp: HostPrivilegeCeilingResponse = fory
        .deserialize(&recv_resp_bytes)
        .expect("fory decode ceiling resp");
    assert_eq!(resp, decoded_resp);
}

#[tokio::test]
async fn test_stage_completion_report_security_grant_denied_serde_json_and_frame_roundtrip() {
    let (mut client, mut server) = duplex(4096);
    let fory = create_fory_runtime().expect("Fory runtime");

    let denied_report = StageCompletionReport {
        job_id: "job-security-violation-501".to_string(),
        stage_id: "priv-escalate-stage".to_string(),
        exit_code: 126,
        duration_secs: 2,
        error_message: Some(
            "Security grant denied: requested memlock limit exceeds host privilege ceiling"
                .to_string(),
        ),
        security_grant_denied: Some(SecurityGrantDenied {
            job_id: "job-security-violation-501".to_string(),
            stage_id: "priv-escalate-stage".to_string(),
            violated_layer: "workflow_override".to_string(),
            offending_field: "memlock_limit_kb".to_string(),
            requested_value: "67108864".to_string(),
            ceiling_value: "29360128".to_string(),
        }),
        traceparent: None,
    };

    // serde_json roundtrip
    let report_json =
        serde_json::to_string(&denied_report).expect("serde_json serialize denied report");
    let report_json_decoded: StageCompletionReport =
        serde_json::from_str(&report_json).expect("serde_json deserialize denied report");
    assert_eq!(denied_report, report_json_decoded);
    assert_eq!(
        report_json_decoded
            .security_grant_denied
            .as_ref()
            .unwrap()
            .offending_field,
        "memlock_limit_kb"
    );
    assert_eq!(
        report_json_decoded
            .security_grant_denied
            .as_ref()
            .unwrap()
            .violated_layer,
        "workflow_override"
    );

    // Apache Fory binary roundtrip
    let report_fory_bytes = fory
        .serialize(&denied_report)
        .expect("fory serialize denied report");
    let report_fory_decoded: StageCompletionReport = fory
        .deserialize(&report_fory_bytes)
        .expect("fory deserialize denied report");
    assert_eq!(denied_report, report_fory_decoded);

    // Full frame-level duplex roundtrip
    let report_hdr = IpcHeader::new(
        MSG_TYPE_STAGE_COMPLETION,
        report_fory_bytes.len() as u32,
        0,
        401,
    );
    write_frame(&mut client, &report_hdr, &report_fory_bytes)
        .await
        .expect("write frame denied report");
    let (recv_hdr, recv_payload) = read_frame(&mut server)
        .await
        .expect("read frame denied report");
    assert_eq!(recv_hdr, report_hdr);
    let decoded_framed_report: StageCompletionReport = fory
        .deserialize(&recv_payload)
        .expect("fory decode framed denied report");
    assert_eq!(denied_report, decoded_framed_report);
    assert!(decoded_framed_report.security_grant_denied.is_some());
}

#[tokio::test]
async fn test_traceparent_propagation_and_backward_compatible_serde_roundtrip() {
    let (mut client, mut server) = duplex(8192);
    let fory = create_fory_runtime().expect("Fory runtime");
    let tp = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());

    // 1. StageDispatchTask with traceparent Some(...)
    let dispatch = StageDispatchTask {
        job_id: "job-otel-01".to_string(),
        stage_id: "test-stage".to_string(),
        stage_name: "Cargo Test".to_string(),
        repo_name: "LMA1980/mu-ci-ipc".to_string(),
        clone_url: "https://github.com/LMA1980/mu-ci-ipc.git".to_string(),
        commit_sha: "1122334455667788".to_string(),
        command: String::new(),
        working_dir: None,
        env_vars: vec![],
        timeout_seconds: 300,
        threads: 4,
        toolchain: None,
        exclusive: false,
        max_concurrent_per_node: 1,
        tasks: vec![],
        sandbox_constraints: SandboxConstraintSpec::default(),
        traceparent: tp.clone(),
        sccache_mode: SccacheMode::default(),
    };

    let dispatch_bytes = fory.serialize(&dispatch).expect("serialize dispatch");
    let dispatch_hdr = IpcHeader::new(MSG_TYPE_STAGE_DISPATCH, dispatch_bytes.len() as u32, 0, 501);
    write_frame(&mut client, &dispatch_hdr, &dispatch_bytes)
        .await
        .expect("write dispatch");
    let (_, recv_dispatch_bytes) = read_frame(&mut server).await.expect("read dispatch");
    let decoded_dispatch: StageDispatchTask = fory
        .deserialize(&recv_dispatch_bytes)
        .expect("deserialize dispatch");
    assert_eq!(decoded_dispatch.traceparent, tp);
    assert_eq!(decoded_dispatch, dispatch);

    // Verify #[serde(default)] backward compatibility when JSON omits traceparent
    let legacy_json = r#"{
        "job_id": "job-legacy",
        "stage_id": "s1",
        "stage_name": "Stage 1",
        "repo_name": "LMA1980/mu-ci-ipc",
        "clone_url": "https://github.com/LMA1980/mu-ci-ipc.git",
        "commit_sha": "deadbeef",
        "command": "cargo test",
        "working_dir": null,
        "env_vars": [],
        "timeout_seconds": 60,
        "threads": 2,
        "toolchain": null,
        "exclusive": false,
        "max_concurrent_per_node": 1,
        "tasks": [],
        "sandbox_constraints": {
            "profile_name": "",
            "backend": "Kernel",
            "read_paths": [],
            "write_paths": [],
            "exec_paths": [],
            "network_mode": "Offline",
            "memlock_limit_kb": 0,
            "max_pids": 0,
            "require_set_ai_thread": false
        }
    }"#;
    let parsed_legacy: StageDispatchTask =
        serde_json::from_str(legacy_json).expect("legacy json without traceparent must parse");
    assert_eq!(parsed_legacy.traceparent, None);

    // 2. StageStarted with traceparent Some(...)
    let started = StageStarted {
        job_id: "job-otel-01".to_string(),
        stage_id: "test-stage".to_string(),
        node_id: "worker-riscv64".to_string(),
        pid: 12345,
        started_at_epoch_ms: 1730000000000,
        traceparent: tp.clone(),
    };
    let started_bytes = fory.serialize(&started).expect("serialize started");
    let started_hdr = IpcHeader::new(MSG_TYPE_STAGE_STARTED, started_bytes.len() as u32, 0, 502);
    write_frame(&mut server, &started_hdr, &started_bytes)
        .await
        .expect("write started");
    let (_, recv_started_bytes) = read_frame(&mut client).await.expect("read started");
    let decoded_started: StageStarted = fory
        .deserialize(&recv_started_bytes)
        .expect("deserialize started");
    assert_eq!(decoded_started.traceparent, tp);
    assert_eq!(decoded_started, started);

    // 3. StageCompletionReport with traceparent Some(...)
    let report = StageCompletionReport {
        job_id: "job-otel-01".to_string(),
        stage_id: "test-stage".to_string(),
        exit_code: 0,
        duration_secs: 15,
        error_message: None,
        security_grant_denied: None,
        traceparent: tp.clone(),
    };
    let report_bytes = fory.serialize(&report).expect("serialize completion");
    let report_hdr = IpcHeader::new(MSG_TYPE_STAGE_COMPLETION, report_bytes.len() as u32, 0, 503);
    write_frame(&mut server, &report_hdr, &report_bytes)
        .await
        .expect("write completion");
    let (_, recv_report_bytes) = read_frame(&mut client).await.expect("read completion");
    let decoded_report: StageCompletionReport = fory
        .deserialize(&recv_report_bytes)
        .expect("deserialize completion");
    assert_eq!(decoded_report.traceparent, tp);
    assert_eq!(decoded_report, report);
}

#[tokio::test]
async fn test_arrow_test_runs_and_bench_runs_batches_over_duplex() {
    let (mut client, mut server) = duplex(16384);

    assert_eq!(test_runs_schema().fields().len(), 12);
    assert_eq!(bench_runs_schema().fields().len(), 14);

    let test_records = vec![
        TestCaseRunRecord {
            timestamp_us: 1730000000000000,
            job_id: "job-201".to_string(),
            stage_id: "unit-tests".to_string(),
            repo_key: "LMA1980/ActionRunner-tools".to_string(),
            branch_key: "main".to_string(),
            commit_sha: "abcdef0123456789".to_string(),
            crate_name: "action-runner-tools".to_string(),
            test_binary: "security_injection_tests".to_string(),
            test_name: "test_cargo_test_adapter".to_string(),
            status: "Passed".to_string(),
            duration_ms: Some(12),
            failure_summary: None,
        },
        TestCaseRunRecord {
            timestamp_us: 1730000000012000,
            job_id: "job-201".to_string(),
            stage_id: "unit-tests".to_string(),
            repo_key: "LMA1980/ActionRunner-tools".to_string(),
            branch_key: "main".to_string(),
            commit_sha: "abcdef0123456789".to_string(),
            crate_name: "action-runner-tools".to_string(),
            test_binary: "security_injection_tests".to_string(),
            test_name: "test_failing_case".to_string(),
            status: "Failed".to_string(),
            duration_ms: Some(4),
            failure_summary: Some("assertion failed: left == right".to_string()),
        },
    ];

    let test_bytes = encode_test_runs_batch(&test_records).expect("encode test runs batch");
    let test_hdr = IpcHeader::new(MSG_TYPE_ARROW_TEST_RUNS, test_bytes.len() as u32, 0, 601);
    write_frame(&mut client, &test_hdr, &test_bytes)
        .await
        .expect("write test runs frame");
    let (recv_test_hdr, recv_test_payload) =
        read_frame(&mut server).await.expect("read test runs frame");
    assert_eq!(recv_test_hdr, test_hdr);
    let decoded_tests = decode_test_runs_batch(&recv_test_payload).expect("decode test runs batch");
    assert_eq!(decoded_tests, test_records);

    let bench_records = vec![
        BenchmarkRunRecord {
            timestamp_us: 1730000001000000,
            job_id: "job-201".to_string(),
            stage_id: "rvv-benches".to_string(),
            repo_key: "LMA1980/k3-kernels-rvv".to_string(),
            branch_key: "main".to_string(),
            commit_sha: "abcdef0123456789".to_string(),
            crate_name: "k3-kernels-rvv".to_string(),
            bench_name: "kernels::gemm_q4_0".to_string(),
            ns_per_iter: 1240.5,
            variance_ns: 35.0,
            throughput_mb_s: Some(4096.0),
            arch: "riscv64".to_string(),
            cpu_cluster: "A100".to_string(),
            vlen_bits: 1024,
        },
        BenchmarkRunRecord {
            timestamp_us: 1730000002000000,
            job_id: "job-201".to_string(),
            stage_id: "rvv-benches".to_string(),
            repo_key: "LMA1980/k3-kernels-rvv".to_string(),
            branch_key: "main".to_string(),
            commit_sha: "abcdef0123456789".to_string(),
            crate_name: "k3-kernels-rvv".to_string(),
            bench_name: "kernels::scalar_fallback".to_string(),
            ns_per_iter: 8420.0,
            variance_ns: 110.0,
            throughput_mb_s: None,
            arch: "riscv64".to_string(),
            cpu_cluster: "X100".to_string(),
            vlen_bits: 256,
        },
    ];

    let bench_bytes = encode_bench_runs_batch(&bench_records).expect("encode bench runs batch");
    let bench_hdr = IpcHeader::new(MSG_TYPE_ARROW_BENCH_RUNS, bench_bytes.len() as u32, 0, 602);
    write_frame(&mut client, &bench_hdr, &bench_bytes)
        .await
        .expect("write bench runs frame");
    let (recv_bench_hdr, recv_bench_payload) = read_frame(&mut server)
        .await
        .expect("read bench runs frame");
    assert_eq!(recv_bench_hdr, bench_hdr);
    let decoded_benches =
        decode_bench_runs_batch(&recv_bench_payload).expect("decode bench runs batch");
    assert_eq!(decoded_benches, bench_records);
}

#[test]
fn test_sccache_mode_serde_and_fory_roundtrip() {
    let fory = create_fory_runtime().expect("Fory runtime");

    // 1. Verify boolean and string variants deserialize properly
    let off_bool: SccacheMode = serde_json::from_str("false").expect("parse false");
    assert_eq!(off_bool, SccacheMode::Off);
    let off_str: SccacheMode = serde_json::from_str("\"false\"").expect("parse \"false\"");
    assert_eq!(off_str, SccacheMode::Off);
    let off_disabled: SccacheMode = serde_json::from_str("\"off\"").expect("parse \"off\"");
    assert_eq!(off_disabled, SccacheMode::Off);

    let try_bool: SccacheMode = serde_json::from_str("true").expect("parse true");
    assert_eq!(try_bool, SccacheMode::Try);
    let try_str: SccacheMode = serde_json::from_str("\"try\"").expect("parse \"try\"");
    assert_eq!(try_str, SccacheMode::Try);
    let try_true_str: SccacheMode = serde_json::from_str("\"true\"").expect("parse \"true\"");
    assert_eq!(try_true_str, SccacheMode::Try);

    let mandatory_str: SccacheMode =
        serde_json::from_str("\"mandatory\"").expect("parse \"mandatory\"");
    assert_eq!(mandatory_str, SccacheMode::Mandatory);

    // 2. Verify Fory roundtrip for all three variants inside StageDispatchTask
    for mode in [SccacheMode::Off, SccacheMode::Try, SccacheMode::Mandatory] {
        let dispatch = StageDispatchTask {
            job_id: "job-sccache-test".to_string(),
            stage_id: "build-stage".to_string(),
            stage_name: "Build Stage".to_string(),
            repo_name: "LMA1980/mu-ci-ipc".to_string(),
            clone_url: "https://github.com/LMA1980/mu-ci-ipc.git".to_string(),
            commit_sha: "1234567890abcdef".to_string(),
            command: "cargo build".to_string(),
            working_dir: None,
            env_vars: vec![],
            timeout_seconds: 300,
            threads: 4,
            toolchain: None,
            exclusive: false,
            max_concurrent_per_node: 1,
            tasks: vec![TypedTaskInvocation {
                task_id: "t1".to_string(),
                tool_name: "cargo-build".to_string(),
                args: vec!["--workspace".to_string()],
                working_dir: None,
                env_vars: vec![],
                sandbox: SandboxConstraintSpec::default(),
                sccache_mode: mode,
            }],
            sandbox_constraints: SandboxConstraintSpec::default(),
            traceparent: None,
            sccache_mode: mode,
        };

        let bytes = fory.serialize(&dispatch).expect("fory serialize");
        let decoded: StageDispatchTask = fory.deserialize(&bytes).expect("fory deserialize");
        assert_eq!(decoded.sccache_mode, mode);
        assert_eq!(decoded.tasks[0].sccache_mode, mode);
    }
}

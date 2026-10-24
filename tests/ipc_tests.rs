use action_runner_ipc::{
    create_fory_runtime, decode_log_batch, decode_metric_batch, encode_log_batch,
    encode_metric_batch, read_frame, write_frame, IpcHeader, LogEntry, MetricEntry,
    StageCancelTask, StageCompletionReport, StageDispatchTask, WorkerHeartbeat,
    WorkerRegistration, MSG_TYPE_ARROW_LOG, MSG_TYPE_ARROW_METRIC, MSG_TYPE_STAGE_CANCEL,
    MSG_TYPE_STAGE_COMPLETION, MSG_TYPE_STAGE_DISPATCH, MSG_TYPE_WORKER_HEARTBEAT,
    MSG_TYPE_WORKER_REGISTRATION,
};
use tokio::io::duplex;

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
    let report_hdr = IpcHeader::new(
        MSG_TYPE_STAGE_COMPLETION,
        report_bytes.len() as u32,
        0,
        3,
    );
    write_frame(&mut client, &report_hdr, &report_bytes)
        .await
        .expect("write report");
    let (recv_report_hdr, recv_report_bytes) = read_frame(&mut server).await.expect("read report");
    assert_eq!(recv_report_hdr, report_hdr);
    let decoded_report: StageCompletionReport = fory
        .deserialize(&recv_report_bytes)
        .expect("decode report");
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
    let decoded_cancel: StageCancelTask = fory
        .deserialize(&recv_cancel_bytes)
        .expect("decode cancel");
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

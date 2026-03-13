# Test Results

## Test 1

```
$ echo -e "2024-01-02 INFO Application started\n2024-01-02 INFO Processing request\n2024-01-02 INFO Request completed" | \
$ az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name logs-plain \
  --name "test-$(date +%s).log" \
  --data @-
Finished[#############################################################]  100.0000%
{
  "client_request_id": "c174b18e-1f33-11f1-a5bb-03c6b007573f",
  "content_md5": "vUjSc2DhODyKpVzQTaXxyA==",
  "date": "2026-03-13T23:24:18+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DE8157A6615673\"",
  "lastModified": "2026-03-13T23:24:18+00:00",
  "request_id": "1cc6c2b0-e01e-0008-4340-b37321000000",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-03-13T23:24:18.6723955Z"
}

### vector shell output
{"blob":"test-1773444257.log","container":"logs-plain","message":"2024-01-02 INFO Application started","timestamp":"2026-03-13T23:24:22.455279731Z"}
{"blob":"test-1773444257.log","container":"logs-plain","message":"2024-01-02 INFO Processing request","timestamp":"2026-03-13T23:24:22.455511618Z"}
{"blob":"test-1773444257.log","container":"logs-plain","message":"2024-01-02 INFO Request completed","timestamp":"2026-03-13T23:24:22.455655152Z"}
```

## Test 2

```
$ echo '{"timestamp":"2024-01-02T12:00:00Z","level":"info","message":"Test log 1"}
{"timestamp":"2024-01-02T12:00:01Z","level":"warn","message":"Test log 2"}
{"timestamp":"2024-01-02T12:00:02Z","level":"error","message":"Test log 3"}' | \
$ az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name logs-json \
  --name "json-test-$(date +%s).log" \
  --data @-
Finished[#############################################################]  100.0000%
{
  "client_request_id": "04680b94-1f34-11f1-a5bb-03c6b007573f",
  "content_md5": "E48hSDWnYnkaBLECIvyixw==",
  "date": "2026-03-13T23:26:10+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DE8157E9443676\"",
  "lastModified": "2026-03-13T23:26:10+00:00",
  "request_id": "e4137489-601e-0064-6a40-b398b6000000",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-03-13T23:26:10.8888694Z"
}

### vector shell output
{"blob":"json-test-1773444369.log","container":"logs-json","level":"info","message":"Test log 1","timestamp":"2024-01-02T12:00:00Z"}
{"blob":"json-test-1773444369.log","container":"logs-json","level":"warn","message":"Test log 2","timestamp":"2024-01-02T12:00:01Z"}
{"blob":"json-test-1773444369.log","container":"logs-json","level":"error","message":"Test log 3","timestamp":"2024-01-02T12:00:02Z"}
```

## Test 3

```
$ echo -e "Compressed log line 1\nCompressed log line 2\nCompressed log line 3" | gzip > /tmp/test.log.gz
$ az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name logs-plain \
  --name "test-$(date +%s).log.gz" \
  --file /tmp/test.log.gz
Finished[#############################################################]  100.0000%
{
  "client_request_id": "1de1edb0-1f34-11f1-a5bb-03c6b007573f",
  "content_md5": "ikhzJ3lwvbAsYKC4nkdp2w==",
  "date": "2026-03-13T23:26:53+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DE815802AC1C8B\"",
  "lastModified": "2026-03-13T23:26:53+00:00",
  "request_id": "b48d740f-b01e-00f1-7140-b37003000000",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-03-13T23:26:53.5138159Z"
}

### vector shell output
{"blob":"test-1773444412.log.gz","container":"logs-plain","message":"Compressed log line 1","timestamp":"2026-03-13T23:27:02.811957026Z"}
{"blob":"test-1773444412.log.gz","container":"logs-plain","message":"Compressed log line 2","timestamp":"2026-03-13T23:27:02.812155574Z"}
{"blob":"test-1773444412.log.gz","container":"logs-plain","message":"Compressed log line 3","timestamp":"2026-03-13T23:27:02.812226432Z"}
```

## Test 4

```
$ echo -e "Line 1\nLine 2\nLine 3" | zstd > /tmp/test.log.zst
$ az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name logs-plain \
  --name "zstd-test-$(date +%s).log.zst" \
  --file /tmp/test.log.zst
Finished[#############################################################]  100.0000%
{
  "client_request_id": "6c11a228-1f34-11f1-a5bb-03c6b007573f",
  "content_md5": "0VdGBlsSHBP9Egqqc10p0w==",
  "date": "2026-03-13T23:29:03+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DE815850DACC85\"",
  "lastModified": "2026-03-13T23:29:04+00:00",
  "request_id": "b7b79221-801e-0043-3041-b38f72000000",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-03-13T23:29:04.6810757Z"
}

### vector shell output
{"blob":"zstd-test-1773444543.log.zst","container":"logs-plain","message":"Line 1","timestamp":"2026-03-13T23:29:11.925046898Z"}
{"blob":"zstd-test-1773444543.log.zst","container":"logs-plain","message":"Line 2","timestamp":"2026-03-13T23:29:11.925189686Z"}
{"blob":"zstd-test-1773444543.log.zst","container":"logs-plain","message":"Line 3","timestamp":"2026-03-13T23:29:11.925253711Z"}
```

## Test 5

```
$ echo '2024-01-02 12:34:56 ERROR Something failed
  at com.example.Service.process(Service.java:45)
  at com.example.Handler.handle(Handler.java:23)
  at com.example.Main.main(Main.java:10)
2024-01-02 12:34:57 INFO Recovery initiated
2024-01-02 12:34:58 INFO Service restarted' | gzip > /tmp/stacktrace.log.gz
$ az storage blob upload \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --container-name logs-multiline \
  --name "stacktrace-$(date +%s).log.gz" \
  --file /tmp/stacktrace.log.gz
Finished[#############################################################]  100.0000%
{
  "client_request_id": "9a231066-1f34-11f1-a5bb-03c6b007573f",
  "content_md5": "bMhDFczVScp14efieN6mEg==",
  "date": "2026-03-13T23:30:21+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DE81587EDCF755\"",
  "lastModified": "2026-03-13T23:30:21+00:00",
  "request_id": "d4078003-701e-008c-3841-b30120000000",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-03-13T23:30:21.8704725Z"
}

### vector shell output
{"blob":"stacktrace-1773444620.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:56 ERROR Something failed\n  at com.example.Service.process(Service.java:45)\n  at com.example.Handler.handle(Handler.java:23)\n  at com.example.Main.main(Main.java:10)","timestamp":"2026-03-13T23:30:26.240267670Z"}
{"blob":"stacktrace-1773444620.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:57 INFO Recovery initiated","timestamp":"2026-03-13T23:30:26.240543397Z"}
{"blob":"stacktrace-1773444620.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:58 INFO Service restarted","timestamp":"2026-03-13T23:30:26.240691253Z"}
```

## Test 6

```
$ kill -SIGTERM $VECTOR_PID

### vector shell output
2026-03-13T23:34:12.000449Z  INFO vector::signal: Signal received. signal="SIGTERM"
2026-03-13T23:34:12.000605Z  INFO vector: Vector has stopped.
2026-03-13T23:34:12.000841Z  INFO source{component_kind="source" component_id=azure_logs_plain component_type=azure_blob}: vector::sources::azure_blob::queue: Shutdown signal received, stopping Azure Blob queue polling.
2026-03-13T23:34:12.000898Z  INFO source{component_kind="source" component_id=azure_logs_json component_type=azure_blob}: vector::sources::azure_blob::queue: Shutdown signal received, stopping Azure Blob queue polling.
2026-03-13T23:34:12.002216Z  INFO vector::topology::running: Shutting down... Waiting on running components. remaining_components="azure_logs_plain, azure_logs_json, azure_logs_multiline, console_output" time_remaining="59 seconds left"
```

# Test Results

## Test 1

```
$ echo -e "2024-01-02 INFO Application started\n2024-01-02 INFO Processing request\n2024-01-02 INFO Request completed" | \
  az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" --container-name logs-plain --name "test-plain-$(date +%s).log" --data @-
{
  "client_request_id": "500b3472-0e2c-11f1-a9aa-f919ee4ce6a6",
  "content_md5": "vUjSc2DhODyKpVzQTaXxyA==",
  "date": "2026-02-20T07:18:11+00:00",
  "etag": "\"0x8DE705034EF616D\"",
  "lastModified": "2026-02-20T07:18:12+00:00",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-02-20T07:18:12.0986989Z"
}

### vector shell output
{"blob":"test-plain-1771571890.log","container":"logs-plain","message":"2024-01-02 INFO Application started","timestamp":"2026-02-20T07:18:15.332418245Z"}
{"blob":"test-plain-1771571890.log","container":"logs-plain","message":"2024-01-02 INFO Processing request","timestamp":"2026-02-20T07:18:15.332533331Z"}
{"blob":"test-plain-1771571890.log","container":"logs-plain","message":"2024-01-02 INFO Request completed","timestamp":"2026-02-20T07:18:15.332590143Z"}
```

## Test 2

```
$ echo '{"timestamp":"2024-01-02T12:00:00Z","level":"info","message":"Test log 1"}
{"timestamp":"2024-01-02T12:00:01Z","level":"warn","message":"Test log 2"}
{"timestamp":"2024-01-02T12:00:02Z","level":"error","message":"Test log 3"}' | \
  az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" --container-name logs-json --name "test-json-$(date +%s).log" --data @-
{
  "client_request_id": "6064e6f6-0e2c-11f1-a9aa-f919ee4ce6a6",
  "content_md5": "E48hSDWnYnkaBLECIvyixw==",
  "date": "2026-02-20T07:18:38+00:00",
  "etag": "\"0x8DE70504535A013\"",
  "lastModified": "2026-02-20T07:18:39+00:00",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-02-20T07:18:39.4035954Z"
}

### vector shell output
{"blob":"test-json-1771571918.log","container":"logs-json","level":"info","message":"Test log 1","timestamp":"2024-01-02T12:00:00Z"}
{"blob":"test-json-1771571918.log","container":"logs-json","level":"warn","message":"Test log 2","timestamp":"2024-01-02T12:00:01Z"}
{"blob":"test-json-1771571918.log","container":"logs-json","level":"error","message":"Test log 3","timestamp":"2024-01-02T12:00:02Z"}
```

## Test 3

```
$ echo -e "Compressed log line 1\nCompressed log line 2\nCompressed log line 3" | gzip > /tmp/test.log.gz
$ az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" --container-name logs-plain --name "test-gzip-$(date +%s).log.gz" --file /tmp/test.log.gz
{
  "client_request_id": "71256b0a-0e2c-11f1-a9aa-f919ee4ce6a6",
  "content_md5": "ikhzJ3lwvbAsYKC4nkdp2w==",
  "date": "2026-02-20T07:19:07+00:00",
  "etag": "\"0x8DE7050561AC6F7\"",
  "lastModified": "2026-02-20T07:19:07+00:00",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-02-20T07:19:07.7479159Z"
}

### vector shell output
{"blob":"test-gzip-1771571946.log.gz","container":"logs-plain","message":"Compressed log line 1","timestamp":"2026-02-20T07:19:12.080371206Z"}
{"blob":"test-gzip-1771571946.log.gz","container":"logs-plain","message":"Compressed log line 2","timestamp":"2026-02-20T07:19:12.080532446Z"}
{"blob":"test-gzip-1771571946.log.gz","container":"logs-plain","message":"Compressed log line 3","timestamp":"2026-02-20T07:19:12.080559325Z"}
```

## Test 4

```
$ echo -e "Line 1\nLine 2\nLine 3" | zstd > /tmp/test.log.zst
$ az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" --container-name logs-plain --name "zstd-test-$(date +%s).log.zst" --file /tmp/test.log.zst
{
  "client_request_id": "80c0c0aa-0e2c-11f1-a9aa-f919ee4ce6a6",
  "content_md5": "0VdGBlsSHBP9Egqqc10p0w==",
  "date": "2026-02-20T07:19:33+00:00",
  "etag": "\"0x8DE705065987CAA\"",
  "lastModified": "2026-02-20T07:19:33+00:00",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-02-20T07:19:33.7375914Z"
}

### vector shell output
{"blob":"zstd-test-1771571972.log.zst","container":"logs-plain","message":"Line 1","timestamp":"2026-02-20T07:19:36.837732357Z"}
{"blob":"zstd-test-1771571972.log.zst","container":"logs-plain","message":"Line 2","timestamp":"2026-02-20T07:19:36.837948459Z"}
{"blob":"zstd-test-1771571972.log.zst","container":"logs-plain","message":"Line 3","timestamp":"2026-02-20T07:19:36.837982533Z"}
```

## Test 5

```
$ cat > /tmp/stacktrace.log << 'EOF'
2024-01-02 12:34:56 ERROR Something failed
  at com.example.Service.process(Service.java:45)
  at com.example.Handler.handle(Handler.java:23)
  at com.example.Main.main(Main.java:10)
2024-01-02 12:34:57 INFO Recovery initiated
2024-01-02 12:34:58 INFO Service restarted
EOF
$ gzip -f /tmp/stacktrace.log
$ az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" --container-name logs-multiline --name "stacktrace-$(date +%s).log.gz" --file /tmp/stacktrace.log.gz
{
  "client_request_id": "911e519c-0e2c-11f1-a9aa-f919ee4ce6a6",
  "content_md5": "zoh4W4oW4tZ5+glb2wrYBg==",
  "date": "2026-02-20T07:20:00+00:00",
  "etag": "\"0x8DE705075EEFC52\"",
  "lastModified": "2026-02-20T07:20:01+00:00",
  "request_server_encrypted": true,
  "version": "2022-11-02",
  "version_id": "2026-02-20T07:20:01.1480146Z"
}

### vector shell output
{"blob":"stacktrace-1771572000.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:56 ERROR Something failed\n  at com.example.Service.process(Service.java:45)\n  at com.example.Handler.handle(Handler.java:23)\n  at com.example.Main.main(Main.java:10)","timestamp":"2026-02-20T07:20:06.163630394Z"}
{"blob":"stacktrace-1771572000.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:57 INFO Recovery initiated","timestamp":"2026-02-20T07:20:06.163860880Z"}
{"blob":"stacktrace-1771572000.log.gz","container":"logs-multiline","message":"2024-01-02 12:34:58 INFO Service restarted","timestamp":"2026-02-20T07:20:06.163930066Z"}
```

## Test 6

```
$ kill -SIGTERM $VECTOR_PID

### vector shell output
2026-02-20T07:21:40.948529Z  INFO vector::signal: Signal received. signal="SIGTERM"
2026-02-20T07:21:40.948800Z  INFO vector: Vector has stopped.
2026-02-20T07:21:40.949317Z  INFO source{component_kind="source" component_id=azure_logs_multiline component_type=azure_blob}: vector::sources::azure_blob::queue: Shutdown signal received, stopping Azure Blob queue polling.
2026-02-20T07:21:40.949499Z  INFO source{component_kind="source" component_id=azure_logs_json component_type=azure_blob}: vector::sources::azure_blob::queue: Shutdown signal received, stopping Azure Blob queue polling.
2026-02-20T07:21:40.949814Z  INFO vector::topology::running: Shutting down... Waiting on running components. remaining_components="azure_logs_json, azure_logs_plain, console_output, azure_logs_multiline" time_remaining="59 seconds left"
```

# Testing Plan - Azure Blob Storage Source (#XXXXX)

## Context

This PR introduces a new Vector source that reads logs from Azure Blob Storage by processing events from an Azure Storage Queue. The implementation aims at supporting the same features as the AWS S3 source, providing Event Grid integration for real-time blob processing.

Key features implemented:
- Compression auto-detection (gzip, zstd) from Content-Type headers and file extensions
- Configurable framing (newline-delimited, character-delimited, bytes, length-delimited, octet-counting)
- Multiline aggregation for stack traces and multi-line logs
- Event metadata (container name, blob name, timestamp)
- Acknowledgement support for reliable delivery

**Known limitation:** Unlike the AWS S3 source, Content-Encoding header is NOT supported for compression detection due to an Azure SDK limitation.

## Plan

You must have an active Azure subscription. The Free Tier is enough to test the Azure Blob source.

Start by deploying Azure infrastructure using Terraform, then build Vector from this PR with the azure_blob feature enabled. We'll use a configuration that connects to Azure Blob Storage via a Storage Queue that receives Event Grid notifications when blobs are created.

For testing, we'll upload various types of blobs (plain text, JSON, compressed, multiline) to Azure Storage containers and verify that Vector processes them correctly. The test configuration will output events to the console where we can inspect the processed data, including metadata fields.

We'll use the Azure CLI (`az storage blob upload`) to upload test blobs, which will trigger Event Grid notifications that are sent to the Storage Queue. Vector will poll the queue, retrieve blob references, download and process the blobs, then acknowledge the queue messages upon successful processing.

## Test Case(s)

### Prerequisites Setup

- Deploy Azure infrastructure:
  ```bash
  cd testing/github-13882
  terraform init
  terraform apply
  export AZURE_STORAGE_CONNECTION_STRING=$(terraform output -raw connection_string)
  ```
- Build Vector with azure_blob feature:
  ```bash
  cargo build --features sources-azure_blob
  ```
- Start Vector:
  ```bash
  cargo run --features sources-azure_blob -- --config testing/github-13882/config.toml
  ```

### Test Cases

1. Plain text logs with newline-delimited framing:
   - Upload plain text logs:
     ```bash
     echo -e "2024-01-02 INFO Application started\n2024-01-02 INFO Processing request\n2024-01-02 INFO Request completed" | \
     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-plain \
       --name "test-$(date +%s).log" \
       --data @-
     ```
   - Vector receives Event Grid notification via queue.
   - Blob is downloaded and processed by `azure_logs_plain` source (bytes codec, no multiline).
   - Three events are emitted (one per line).
   - Each event contains metadata: `container="logs-plain"`, `blob="test-*.log"`, `timestamp`.
   - Queue message is acknowledged and deleted.

2. JSON logs with JSON codec:
   - Upload JSON-formatted logs:
     ```bash
     echo '{"timestamp":"2024-01-02T12:00:00Z","level":"info","message":"Test log 1"}
     {"timestamp":"2024-01-02T12:00:01Z","level":"warn","message":"Test log 2"}
     {"timestamp":"2024-01-02T12:00:02Z","level":"error","message":"Test log 3"}' | \
     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-json \
       --name "json-test-$(date +%s).log" \
       --data @-
     ```
   - Vector processes blob with `azure_logs_json` source (JSON codec).
   - Three events emitted.
   - JSON fields (`timestamp`, `level`, `message`) are parsed into event structure.
   - Source metadata fields are added: `container="logs-json"`.

3. Gzip compression auto-detection from file extension:
   - Upload gzip-compressed plain text blob with `.gz` extension:
     ```bash
     echo -e "Compressed log line 1\nCompressed log line 2\nCompressed log line 3" | gzip > /tmp/test.log.gz
     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-plain \
       --name "test-$(date +%s).log.gz" \
       --file /tmp/test.log.gz
     ```
   - Compression auto-detected from `.gz` extension (compression=auto by default).
   - Blob automatically decompressed during processing.
   - Three events emitted (one per line after decompression).
   - Processed by `azure_logs_plain` source with bytes codec.

4. Zstd compression support:
   - Upload zstd-compressed plain text blob:
     ```bash
     echo -e "Line 1\nLine 2\nLine 3" | zstd > /tmp/test.log.zst
     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-plain \
       --name "zstd-test-$(date +%s).log.zst" \
       --file /tmp/test.log.zst
     ```
   - Zstd compression detected from `.zst` extension.
   - Blob decompressed using zstd algorithm.
   - Three events emitted (one per line).
   - Processed by `azure_logs_plain` source.

5. Multiline aggregation for stack traces:
   - Upload logs with stack traces that should be aggregated:
     ```bash
     echo '2024-01-02 12:34:56 ERROR Something failed
       at com.example.Service.process(Service.java:45)
       at com.example.Handler.handle(Handler.java:23)
       at com.example.Main.main(Main.java:10)
     2024-01-02 12:34:57 INFO Recovery initiated
     2024-01-02 12:34:58 INFO Service restarted' | gzip > /tmp/stacktrace.log.gz

     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-multiline \
       --name "stacktrace-$(date +%s).log.gz" \
       --file /tmp/stacktrace.log.gz
     ```
   - Processed by `azure_logs_multiline` source (bytes codec WITH multiline config).
   - Compression auto-detected and decompressed.
   - Multiline configuration aggregates continuation lines (starting with whitespace).
   - First event contains the ERROR line plus all three stack trace lines.
   - Second event contains the INFO Recovery line.
   - Third event contains the INFO Service restarted line.
   - Total: 3 events (not 6).

6. Graceful shutdown:
   - Start Vector, upload a test blob, then send SIGTERM:
     ```bash
     # Start Vector
     cargo run --features sources-azure_blob -- --config testing/github-13882/config.toml &
     VECTOR_PID=$!

     # Upload a small test blob
     echo -e "Line 1\nLine 2\nLine 3" | \
     az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-plain \
       --name "shutdown-test.log" \
       --data @-

     # Wait a moment, then stop Vector
     sleep 5
     kill -SIGTERM $VECTOR_PID
     ```
   - Vector receives SIGTERM and begins graceful shutdown.
   - No panics or errors during shutdown.
   - Vector exits with status code 0.

### Cleanup

- After testing, destroy Azure resources to avoid ongoing costs:
  ```bash
  cd testing/github-13882
  terraform destroy
  ```

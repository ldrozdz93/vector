New `azure_blob` source for reading logs from Azure Blob Storage

A new `azure_blob` source has been added that allows Vector to read logs from Azure Blob Storage containers. The source processes events from Azure Storage Queues to detect when new blobs are created or modified, then reads and decodes the blob contents. The design followed an intended feature parity with AWS S3 source.

Key features:

- Supports connection string authentication
- Automatically processes Azure Event Grid notifications via Storage Queue
- Configurable compression support (gzip, zstd) with auto-detection from file extension (Content-Encoding detection NOT supported)
- Configurable framing for splitting blob contents (newline-delimited, character-delimited, bytes, etc.)
- Multiline aggregation support for combining related log lines (stack traces, multi-line JSON, etc.)
- Configurable decoding for various log formats
- Automatic enrichment of events with container, blob, and timestamp metadata
- Support for acknowledgements

This enables efficient log collection from Azure Blob Storage without polling, making it suitable for high-throughput logging scenarios.

authors: ldrozdz93 kwapik aq1l

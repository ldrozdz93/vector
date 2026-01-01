New `azure_blob` source for reading logs from Azure Blob Storage

A new `azure_blob` source has been added that allows Vector to read logs from Azure Blob Storage containers. The source processes events from Azure Storage Queues to detect when new blobs are created or modified, then reads and decodes the blob contents.

Key features:
- Supports connection string authentication
- Automatically processes Azure Event Grid notifications via Storage Queue
- Configurable decoding for various log formats
- Comprehensive error handling and operational metrics
- Support for acknowledgements to ensure reliable message processing
- Efficient streaming of blob contents without loading entire files into memory

This enables efficient log collection from Azure Blob Storage without polling, making it suitable for high-throughput logging scenarios.

package metadata

components: sources: azure_blob: {
	title: "Azure Blob Storage"

	features: {
		auto_generated:   true
		acknowledgements: true
		multiline: enabled: true
		collect: {
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        true
				enabled_by_scheme:      true
			}
			checkpoint: enabled: false
			proxy: enabled:      true
			from: service:       services.azure_blob
		}
	}

	classes: {
		commonly_used: true
		deployment_roles: ["aggregator", "daemon", "sidecar"]
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "stream"
		stateful:      true
	}

	support: {
		requirements: [
			"""
				The Azure Blob Storage source requires an Azure Storage Queue configured to
				receive Event Grid notifications for the desired Azure Blob Storage container.
				The queue should be subscribed to BlobCreated events from the storage account.
				""",
		]
		warnings: []
		notices: []
	}

	installation: {
		platform_name: null
	}

	configuration: generated.components.sources.azure_blob.configuration

	output: {
		logs: object: {
			description: "A line from an Azure Blob Storage object."
			fields: {
				message: {
					description: "A line from the blob."
					required:    true
					type: string: {
						examples: ["53.126.150.246 - - [01/Oct/2020:11:25:58 -0400] \"GET /disintermediate HTTP/2.0\" 401 20308"]
					}
				}
				timestamp: fields._current_timestamp & {
					description: "The timestamp of when the event was ingested. Defaults to the current timestamp."
				}
				source_type: {
					description: "The name of the source type."
					required:    true
					type: string: {
						examples: ["azure_blob"]
					}
				}
				ingest_timestamp: {
					description: "The timestamp of when the blob was ingested by Vector."
					required:    true
					type: string: {
						examples: ["2020-10-26T12:34:56.789Z"]
					}
				}
			}
		}
		metrics: "": {
			description: "Metric events that may be emitted by this source."
		}
		traces: "": {
			description: "Trace events that may be emitted by this source."
		}
	}

	how_it_works: {
		event_grid: {
			title: "Azure Event Grid Integration"
			body:  """
				This source uses Azure Event Grid to be notified of new blobs in Azure Blob Storage.
				When a blob is created or modified in the configured container, an Event Grid notification
				is sent to an Azure Storage Queue. Vector polls this queue for events and processes
				the blobs referenced in those events.

				This approach is more efficient than polling the storage container directly and ensures
				that Vector processes blobs as soon as they are available.

				To set up Event Grid notifications:

				1. Create an Azure Storage Queue in your storage account
				2. Create an Event Grid subscription for your storage account
				3. Configure the subscription to filter for BlobCreated events
				4. Set the endpoint to the Azure Storage Queue
				5. Configure Vector with the queue name and connection string

				For more information, see the [Azure Event Grid documentation](\(urls.azure_event_grid)).
				"""
		}

		events: {
			title: "Handling events from the `azure_blob` source"
			body:  """
				This source behaves very similarly to the `file` source in that
				it will output one event per line (unless the `multiline`
				configuration option is used).

				You will commonly want to use [transforms](\(urls.vector_transforms)) to
				parse the data. For example, to parse JSON logs from Azure Blob Storage:

				```toml
				[transforms.json_parser]
				type = "remap"
				inputs = ["azure_blob"]
				source = '''
				. = parse_json!(string!(.message))
				'''
				```

				To parse structured logs with a specific format:

				```toml
				[transforms.parse_logs]
				type = "remap"
				inputs = ["azure_blob"]
				drop_on_error = false
				source = '''
				parsed = parse_regex!(.message, r'^(?P<timestamp>[^ ]+) (?P<level>[^ ]+) (?P<message>.+)$')
				.timestamp = parsed.timestamp
				.level = parsed.level
				.message = parsed.message
				'''
				```
				"""
		}

		queue_processing: {
			title: "Queue Message Processing"
			body:  """
				Vector polls the configured Azure Storage Queue for Event Grid messages about blob events.
				When a message is received, Vector:

				1. Decodes the Event Grid notification from the queue message
				2. Extracts the blob container and path from the event
				3. Downloads the blob contents using the Azure Storage SDK
				4. Streams the blob line-by-line to the configured decoding pipeline
				5. Deletes the queue message after successful processing

				If acknowledgements are enabled, the queue message is only deleted after downstream
				components have confirmed delivery. This ensures at-least-once delivery semantics.

				The source automatically handles:
				- Blob downloads with streaming to handle large files efficiently
				- 404 errors for blobs that no longer exist
				- Queue message visibility timeouts and retries
				- Graceful shutdown without losing events
				"""
		}

		authentication: {
			title: "Authentication"
			body:  """
				The Azure Blob Storage source supports authentication via connection string.
				The connection string should include the storage account name, access key, and endpoint.

				Example connection string:

				```text
				DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net
				```

				For production use, store the connection string securely and reference it via environment variables:

				```toml
				[sources.azure_logs]
				type = "azure_blob"
				connection_string = "${AZURE_STORAGE_CONNECTION_STRING}"
				container_name = "logs"

				[sources.azure_logs.queue]
				queue_name = "eventgrid"
				```
				"""
		}
	}

	telemetry: metrics: {
		component_errors_total:                      components.sources.internal_metrics.output.metrics.component_errors_total
		component_received_bytes_total:              components.sources.internal_metrics.output.metrics.component_received_bytes_total
		component_received_event_bytes_total:        components.sources.internal_metrics.output.metrics.component_received_event_bytes_total
		component_received_events_total:             components.sources.internal_metrics.output.metrics.component_received_events_total
	}
}

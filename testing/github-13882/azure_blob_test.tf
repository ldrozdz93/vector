terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource group for testing
resource "azurerm_resource_group" "vector_test" {
  name     = "vector-azure-blob-test"
  location = "East US"
}

# Storage account
resource "azurerm_storage_account" "vector_test" {
  name                     = "vectorblobtest${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.vector_test.name
  location                 = azurerm_resource_group.vector_test.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  # Enable blob versioning and change feed for better testing
  blob_properties {
    change_feed_enabled = true
    versioning_enabled  = true
  }
}

# Random suffix for unique naming
resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

# Blob container for JSON logs
resource "azurerm_storage_container" "logs_json" {
  name                  = "logs-json"
  storage_account_name  = azurerm_storage_account.vector_test.name
  container_access_type = "private"
}

# Blob container for plain text logs (no multiline)
resource "azurerm_storage_container" "logs_plain" {
  name                  = "logs-plain"
  storage_account_name  = azurerm_storage_account.vector_test.name
  container_access_type = "private"
}

# Blob container for plain text logs with multiline aggregation
resource "azurerm_storage_container" "logs_multiline" {
  name                  = "logs-multiline"
  storage_account_name  = azurerm_storage_account.vector_test.name
  container_access_type = "private"
}

# Storage queues for Event Grid messages (one per container)
resource "azurerm_storage_queue" "queue_plain" {
  name                 = "queue-plain"
  storage_account_name = azurerm_storage_account.vector_test.name
}

resource "azurerm_storage_queue" "queue_json" {
  name                 = "queue-json"
  storage_account_name = azurerm_storage_account.vector_test.name
}

resource "azurerm_storage_queue" "queue_multiline" {
  name                 = "queue-multiline"
  storage_account_name = azurerm_storage_account.vector_test.name
}

# Event Grid System Topic for the storage account
resource "azurerm_eventgrid_system_topic" "storage" {
  name                   = "vector-blob-events"
  resource_group_name    = azurerm_resource_group.vector_test.name
  location               = azurerm_resource_group.vector_test.location
  source_arm_resource_id = azurerm_storage_account.vector_test.id
  topic_type             = "Microsoft.Storage.StorageAccounts"
}

# Event Grid subscription for JSON logs container
resource "azurerm_eventgrid_event_subscription" "logs_json" {
  name  = "logs-json-to-queue"
  scope = azurerm_storage_account.vector_test.id

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.vector_test.id
    queue_name         = azurerm_storage_queue.queue_json.name
  }

  included_event_types = [
    "Microsoft.Storage.BlobCreated",
  ]

  subject_filter {
    subject_begins_with = "/blobServices/default/containers/logs-json/"
  }

  depends_on = [
    azurerm_storage_queue.queue_json,
    azurerm_eventgrid_system_topic.storage
  ]
}

# Event Grid subscription for plain text logs container
resource "azurerm_eventgrid_event_subscription" "logs_plain" {
  name  = "logs-plain-to-queue"
  scope = azurerm_storage_account.vector_test.id

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.vector_test.id
    queue_name         = azurerm_storage_queue.queue_plain.name
  }

  included_event_types = [
    "Microsoft.Storage.BlobCreated",
  ]

  subject_filter {
    subject_begins_with = "/blobServices/default/containers/logs-plain/"
  }

  depends_on = [
    azurerm_storage_queue.queue_plain,
    azurerm_eventgrid_system_topic.storage
  ]
}

# Event Grid subscription for multiline logs container
resource "azurerm_eventgrid_event_subscription" "logs_multiline" {
  name  = "logs-multiline-to-queue"
  scope = azurerm_storage_account.vector_test.id

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.vector_test.id
    queue_name         = azurerm_storage_queue.queue_multiline.name
  }

  included_event_types = [
    "Microsoft.Storage.BlobCreated",
  ]

  subject_filter {
    subject_begins_with = "/blobServices/default/containers/logs-multiline/"
  }

  depends_on = [
    azurerm_storage_queue.queue_multiline,
    azurerm_eventgrid_system_topic.storage
  ]
}

# Outputs
output "storage_account_name" {
  value       = azurerm_storage_account.vector_test.name
  description = "Storage account name"
}

output "connection_string" {
  value       = azurerm_storage_account.vector_test.primary_connection_string
  description = "Storage account connection string"
  sensitive   = true
}

output "container_logs_json" {
  value       = azurerm_storage_container.logs_json.name
  description = "Blob container name for JSON logs"
}

output "container_logs_plain" {
  value       = azurerm_storage_container.logs_plain.name
  description = "Blob container name for plain text logs"
}

output "container_logs_multiline" {
  value       = azurerm_storage_container.logs_multiline.name
  description = "Blob container name for multiline logs"
}

output "queue_name_plain" {
  value       = azurerm_storage_queue.queue_plain.name
  description = "Storage queue name for plain logs"
}

output "queue_name_json" {
  value       = azurerm_storage_queue.queue_json.name
  description = "Storage queue name for JSON logs"
}

output "queue_name_multiline" {
  value       = azurerm_storage_queue.queue_multiline.name
  description = "Storage queue name for multiline logs"
}

output "resource_group" {
  value       = azurerm_resource_group.vector_test.name
  description = "Resource group name"
}

output "setup_instructions" {
  sensitive = true
  value = <<-EOT

  Azure Blob Storage Test Environment Created!

  To use with Vector:
  1. Export the connection string:
     export AZURE_STORAGE_CONNECTION_STRING="${azurerm_storage_account.vector_test.primary_connection_string}"

  2. Run Vector with the test config:
     cargo run -- --config testing/github-13882/config.toml

  3. Upload test blobs to trigger events:
     # JSON logs
     echo '{"level":"info","message":"Test log"}' | az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-json --name test-$(date +%s).log --data @-

     # Plain text logs
     echo -e "Plain text log 1\nPlain text log 2" | az storage blob upload \
       --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-plain --name test-$(date +%s).log --data @-

     # Multiline logs (compressed)
     echo -e "2024-01-02 ERROR Failed\n  at line 1\n  at line 2" | gzip > /tmp/test.log.gz
     az storage blob upload --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
       --container-name logs-multiline --name test-$(date +%s).log.gz --file /tmp/test.log.gz

  4. Check Vector output for processed events

  To destroy resources:
     terraform destroy
  EOT
}

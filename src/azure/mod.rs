//! Shared Azure client infrastructure used by both sources and sinks.

/// Client construction: authentication, credentials, and HTTP transport assembly.
pub mod client;
/// Azure Storage connection-string parsing and endpoint resolution.
pub mod connection_string;
/// SharedKey request signing for the new Azure SDK pipeline.
pub mod shared_key_policy;

#[cfg(feature = "azure")]
use std::sync::Arc;

#[cfg(feature = "azure")]
use azure_core_for_storage::RetryOptions;
#[cfg(feature = "azure")]
use azure_storage::{CloudLocation, ConnectionString};
#[cfg(feature = "azure")]
use azure_storage_blobs::prelude::*;

/// Builds an Azure Blob Storage container client from a connection string.
///
/// Supports both custom blob endpoints (e.g. Azurite) and standard Azure Commercial endpoints.
#[cfg(feature = "azure")]
pub fn build_client(
    connection_string: String,
    container_name: String,
) -> crate::Result<Arc<ContainerClient>> {
    let client = {
        let connection_string = ConnectionString::new(&connection_string)?;
        let account_name = connection_string
            .account_name
            .ok_or("Account name missing in connection string")?;

        match connection_string.blob_endpoint {
            // When the blob_endpoint is provided, we use the Custom CloudLocation
            // which takes the full URI directly instead of deriving it from the
            // account_name. The account_name is still required for authentication
            // and request signing.
            Some(uri) => ClientBuilder::with_location(
                CloudLocation::Custom {
                    uri: uri.to_string(),
                    account: account_name.to_string(),
                },
                connection_string.storage_credentials()?,
            ),
            // Without a valid blob_endpoint in the connection_string, assume we are in Azure
            // Commercial (AzureCloud location) and create a default Blob Storage Client that
            // builds the API endpoint location using the account_name as input
            None => ClientBuilder::new(account_name, connection_string.storage_credentials()?),
        }
        .retry(RetryOptions::none())
        .container_client(container_name)
    };
    Ok(Arc::new(client))
}

/// Builds an Azure Storage Queue client from a connection string.
///
/// Supports both custom queue endpoints (e.g. Azurite) and standard Azure Commercial endpoints.
#[cfg(feature = "sources-azure_blob")]
pub fn build_queue_client(
    connection_string: &str,
    queue_name: String,
) -> crate::Result<Arc<azure_storage_queues::QueueClient>> {
    use azure_storage_queues::QueueServiceClientBuilder;

    let service_client = {
        let connection_string = ConnectionString::new(connection_string)?;
        let account_name = connection_string
            .account_name
            .ok_or("Account name missing in connection string")?;

        match connection_string.queue_endpoint {
            Some(uri) => QueueServiceClientBuilder::with_location(
                CloudLocation::Custom {
                    uri: uri.to_string(),
                    account: account_name.to_string(),
                },
                connection_string.storage_credentials()?,
            ),
            None => QueueServiceClientBuilder::new(
                account_name,
                connection_string.storage_credentials()?,
            ),
        }
        .retry(RetryOptions::none())
        .build()
    };

    let client = service_client.queue_client(queue_name);
    Ok(Arc::new(client))
}

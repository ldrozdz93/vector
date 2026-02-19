//! Shared Azure client infrastructure used by both sources and sinks.

use std::sync::Arc;

use azure_core_for_storage::RetryOptions;
use azure_storage::{CloudLocation, ConnectionString};
use azure_storage_blobs::prelude::*;

/// Builds an Azure Blob Storage container client from a connection string.
///
/// Supports both custom blob endpoints (e.g. Azurite) and standard Azure Commercial endpoints.
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
            // When the blob_endpoint is provided, we use the Custom CloudLocation since it is
            // required to contain the full URI to the blob storage API endpoint, this means
            // that account_name is not required to exist in the connection_string since
            // account_name is only used with the default CloudLocation in the Azure SDK to
            // generate the storage API endpoint
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

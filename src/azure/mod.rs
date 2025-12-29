//! Shared functionality for the Azure components.
use std::sync::Arc;

use azure_core::{auth::TokenCredential, new_http_client, HttpClient, RetryOptions};
use azure_identity::{
    AutoRefreshingTokenCredential, ClientSecretCredential, DefaultAzureCredential,
    TokenCredentialOptions,
};
use azure_storage::{prelude::*, CloudLocation, ConnectionString};
use azure_storage_blobs;
use azure_storage_queues;
use serde_with::serde_as;

use vector_lib::configurable::configurable_component;

/// Stores credentials used to build Azure Clients.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct ClientCredentials {
    /// Check how to get Tenant ID in [the docs][docs].
    ///
    /// [docs]: https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id
    tenant_id: String,

    /// Check how to get Client ID in [the docs][docs].
    ///
    /// [docs]: https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#add-credentials
    client_id: String,

    /// Check how to get Client Secret in [the docs][docs].
    ///
    /// [docs]: https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app#add-credentials
    client_secret: String,
}

/// Builds Azure Storage Container Client.
///
/// To authenticate only **one** of the following should be set:
/// 1. `connection_string`
/// 2. `storage_account` - optionally you can set `client_credentials` to provide credentials,
///    if `client_credentials` is None, [`DefaultAzureCredential`][dac] would be used.
///
/// [dac]: https://docs.rs/azure_identity/0.17.0/azure_identity/struct.DefaultAzureCredential.html
pub fn build_container_client(
    connection_string: Option<String>,
    storage_account: Option<String>,
    container_name: String,
    endpoint: Option<String>,
    client_credentials: Option<ClientCredentials>,
) -> crate::Result<Arc<azure_storage_blobs::prelude::ContainerClient>> {
    let client;
    match (connection_string, storage_account) {
        (Some(connection_string_p), None) => {
            let connection_string = ConnectionString::new(&connection_string_p)?;

            client = match connection_string.blob_endpoint {
                Some(uri) => azure_storage_blobs::prelude::ClientBuilder::with_location(
                    CloudLocation::Custom {
                        uri: uri.to_string(),
                    },
                    connection_string.storage_credentials()?,
                ),
                None => azure_storage_blobs::prelude::ClientBuilder::new(
                    connection_string
                        .account_name
                        .ok_or("Account name missing in connection string")?,
                    connection_string.storage_credentials()?,
                ),
            }
            .retry(RetryOptions::none())
            .container_client(container_name);
        }
        (None, Some(storage_account_p)) => {
            let creds: Arc<dyn TokenCredential> = match client_credentials {
                Some(client_credentials_p) => token_credential_from_client_credentials(client_credentials_p),
                None => std::sync::Arc::new(DefaultAzureCredential::default()) as _,
            };
            let auto_creds = std::sync::Arc::new(AutoRefreshingTokenCredential::new(creds));
            let storage_credentials = StorageCredentials::token_credential(auto_creds);

            client = match endpoint {
                Some(endpoint) => azure_storage_blobs::prelude::ClientBuilder::with_location(
                    CloudLocation::Custom { uri: endpoint },
                    storage_credentials,
                ),
                None => azure_storage_blobs::prelude::ClientBuilder::new(
                    storage_account_p,
                    storage_credentials,
                ),
            }
            .retry(RetryOptions::none())
            .container_client(container_name);
        }
        (None, None) => {
            return Err("Either `connection_string` or `storage_account` has to be provided".into())
        }
        (Some(_), Some(_)) => {
            return Err(
                "`connection_string` and `storage_account` can't be provided at the same time"
                    .into(),
            )
        }
    }
    Ok(std::sync::Arc::new(client))
}

fn token_credential_from_client_credentials(
    client_credentials: ClientCredentials,
) -> Arc<dyn TokenCredential> {
    let http_client: Arc<dyn HttpClient> = new_http_client();
    let options = TokenCredentialOptions::default();
    std::sync::Arc::new(ClientSecretCredential::new(
        Arc::<dyn azure_core::HttpClient>::clone(&http_client),
        client_credentials.tenant_id,
        client_credentials.client_id,
        client_credentials.client_secret,
        options,
    )) as _
}

/// Builds Azure Queue Service Client.
///
/// To authenticate only **one** of the following should be set:
/// 1. `connection_string`
/// 2. `storage_account` - optionally you can set `client_credentials` to provide credentials,
///    if `client_credentials` is None, [`DefaultAzureCredential`][dac] would be used.
///
/// [dac]: https://docs.rs/azure_identity/0.17.0/azure_identity/struct.DefaultAzureCredential.html
pub fn build_queue_client(
    connection_string: Option<String>,
    storage_account: Option<String>,
    queue_name: String,
    endpoint: Option<String>,
    client_credentials: Option<ClientCredentials>,
) -> crate::Result<Arc<azure_storage_queues::QueueClient>> {
    let client;
    match (connection_string, storage_account) {
        (Some(connection_string_p), None) => {
            let connection_string = ConnectionString::new(&connection_string_p)?;

            client = match connection_string.queue_endpoint {
                Some(uri) => azure_storage_queues::QueueServiceClientBuilder::with_location(
                    CloudLocation::Custom {
                        uri: uri.to_string(),
                    },
                    connection_string.storage_credentials()?,
                ),
                None => azure_storage_queues::QueueServiceClientBuilder::new(
                    connection_string
                        .account_name
                        .ok_or("Account name missing in connection string")?,
                    connection_string.storage_credentials()?,
                ),
            }
            .retry(RetryOptions::none())
            .build()
            .queue_client(queue_name);
        }
        (None, Some(storage_account_p)) => {
            let creds: Arc<dyn TokenCredential> = match client_credentials {
                Some(client_credentials_p) => token_credential_from_client_credentials(client_credentials_p),
                None => std::sync::Arc::new(DefaultAzureCredential::default()) as _,
            };
            let auto_creds = std::sync::Arc::new(AutoRefreshingTokenCredential::new(creds));
            let storage_credentials = StorageCredentials::token_credential(auto_creds);

            client = match endpoint {
                Some(endpoint) => azure_storage_queues::QueueServiceClientBuilder::with_location(
                    CloudLocation::Custom { uri: endpoint },
                    storage_credentials,
                ),
                None => azure_storage_queues::QueueServiceClientBuilder::new(
                    storage_account_p,
                    storage_credentials,
                ),
            }
            .retry(RetryOptions::none())
            .build()
            .queue_client(queue_name);
        }
        (None, None) => {
            return Err("Either `connection_string` or `storage_account` has to be provided".into())
        }
        (Some(_), Some(_)) => {
            return Err(
                "`connection_string` and `storage_account` can't be provided at the same time"
                    .into(),
            )
        }
    }
    Ok(std::sync::Arc::new(client))
}

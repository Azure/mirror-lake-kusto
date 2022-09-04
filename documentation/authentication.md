# Authentication

This article covers the different authentication mechanism available in Kusto Mirror Table.

## Resources

There are three resources requiring authentication in Kusto Mirror Table:

1.  Azure Data Explorer Database where the Delta Table is ingested.
1.  Apache Delta Lake table blobs
1.  Checkpoint blob

On top of that, there are two applications accessing those resources:  Kusto Mirror Table itself and Azure Data Explorer.

This is because Kusto Mirror Table runs as a stand alone CLI.  Certain part of the mirroring processed are done by the CLI while others (e.g. ingestion) is delegated to Azure Data Explorer.

##  Authentication Mechanisms

Kusto Mirror Table (KMT) supports a few authentication mechanisms.  The user can explicitely choose the authentication mechanism by specifying it in the Kusto Cluster Ingestion Connection String.  This is done using standard [Kusto Connection String notation](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto).

Depending on the choosen mechanism, KMT will infer an authentication mechanism for the Delta Lake blobs and the checkpoint blob as follow:

Cluster|Can create SAS Token?|KMT to Checkpoint|ADX to Checkpoint|KMT to Delta Lake|ADX to Delta Lake
-|-|-|-|-|-
Azure CLI (default)|Yes|SAS token|SAS token|SAS token|SAS token
Azure CLI (default)|No|Azure CLI|Impersonation|Azure CLI|Cluster system identity
Service Principal|Yes|SAS token|SAS token|SAS token|SAS token
Service Principal|No|Service Principal|Impersonation|Service Principal|Cluster system identity
Managed Service Identity|N/A|Unsupported|Unsupported|Unsupported|Unsupported

As we can see, KMT will try to create SAS tokens for the other resources if the identity used for the Cluster can.  Otherwise it falls back to using AAD Authentication.  This is to simplify scenarios since in order to use AAD everywhere, we need to configure the cluster and 2 storage accounts.


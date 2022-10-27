# Authentication

This article covers the different authentication mechanism available in Mirror Lake Kusto.

## Resources

There are three resources requiring authentication in Mirror Lake Kusto:

1.  Azure Data Explorer Database where the Delta Table is ingested.
1.  Apache Delta Lake table blobs
1.  Checkpoint blob

On top of that, there are two applications accessing those resources:  Mirror Lake Kusto itself and Azure Data Explorer.

This is because Mirror Lake Kusto runs as a stand alone CLI.  Certain part of the mirroring processed are done by the CLI (e.g. reading Delta Lake transaction log) while others (e.g. ingestion) is delegated to Azure Data Explorer.

##  Authentication Mechanisms

Mirror Lake Kusto (MLK) supports a few authentication mechanisms.  The user can explicitely choose the authentication mechanism by specifying it in the Kusto Cluster Ingestion Connection String.  This is done using standard [Kusto Connection String notation](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto).

Depending on the choosen mechanism, MLK will infer an authentication mechanism for the Delta Lake blobs and the checkpoint blob as follow:

Cluster|MLK to Checkpoint|ADX to Checkpoint|MLK to Delta Lake|ADX to Delta Lake
-|-|-|-|-
User Auth (default)|Azure Default|Impersonate|Azure Default|Impersonate
Service Principal|Service Principal|Impersonate|Service Principal|Service Principal

## Connection string example

The simplest example is with the user authentication mechanism, where the connection string can simply be the ingestion URL itself:

```
https://ingest-mycluster.region.kusto.windows.net
```

Using service principal looks like this:

```
Data Source=https://ingest-mycluster.region.kusto.windows.net;Application Client Id=appid;Application Key=****;Authority Id=tennantid
```

The parameters are defined in [this documentation](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto#application-authentication-properties).

In both cases, `AAD Federated Security` and `dSTS Federated Security` are set automatically and can be omitted.
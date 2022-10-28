# Mirror Lake Kusto

![Lake](documentation/media/Lake.png)

[![Continuous Build / Unit Test](https://github.com/Azure/mirror-lake-kusto/actions/workflows/continuous-build.yaml/badge.svg)](https://github.com/Azure/mirror-lake-kusto/actions/workflows/continuous-build.yaml)
[![Integration tests](https://github.com/Azure/mirror-lake-kusto/actions/workflows/int_tests.yaml/badge.svg)](https://github.com/Azure/mirror-lake-kusto/actions/workflows/int_tests.yaml)

Mirror Lake Kusto is a Command Line Interface (CLI) tool allowing you to keep an [Apache Delta Lake table](https://delta.io/) and a [Kusto Table](https://learn.microsoft.com/en-us/azure/data-explorer/data-explorer-overview) in sync so the Kusto table becomes a *mirror* of the Delta table.

There are many options and authentication methods, but a straightforward example is:

```
mirror-lake-kusto -s <Delta Table ADLS folder URL> -i <Kusto Cluster Ingestion URL> -d <Kusto DB> -t <Kusto Table> -c <Checkpoint ADLS folder URL>
```

The CLI will keep the two tables in sync by ingesting and deleting data in Kusto.  **No duplication, no data loss.**
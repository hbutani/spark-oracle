# Spark_On_Oracle

- Currently Data Lakes comprising of Oracle Data Warehouse and Apache Spark
  - Have **separate Data Catalogs**. Even if they access the same data on Object Store.
  - Applications build entirely on Spark, have to **compensate for gaps in Data Management.**
  - Applications that federate across Spark and Oracle usually suffer from
    **inefficient data movement.**
  - Operating Spark clusters are expensive, because of lack of administration tooling
    and gaps in data management. **So price-performance advantages of Spark are overstated.**

![current deployments](https://github.com/oracle/spark-oracle/wiki/uploads/currentDeploymentDrawbacks.png)

This project fixes these issues:
- A single Catalog = Oracle Data Dictionary
- Oracle responsible for Data Management: Consistency, Isolation, Security, storage layout,
  data lifecycle…
  - Even data on Object Store managed by Oracle as External Tables
- But support full Spark programming model
- **Spark on Oracle characteristics**
  - Fully pushdown SQL workloads: Query, DML on all tables; DDL for external tables
  - Push SQL operations of other workloads
  - Surface Oracle capabilities like Machine learning, Streaming in Spark programming model
  - Co-processor on Oracle instances to run certain kinds of  scala code.
    - Co-processors are isolated and limited. So easy to manage
- Enable simpler, smaller Spark clusters

![spark on oracle](https://github.com/oracle/spark-oracle/wiki/uploads/spark-on-oracle.png)

**Feature summary:**
- Catalog integration(see [this page](https://github.com/oracle/spark-oracle/wiki/Oracle-Catalog) for details)
- Significant support for SQL pushdown(see [Operator](https://github.com/oracle/spark-oracle/wiki/Operator-Translation)
  and [Expression](https://github.com/oracle/spark-oracle/wiki/Expression-Translation) translation pages)
  to the extent that more than 95(of 99) [TPCDS queries](https://github.com/oracle/spark-oracle/wiki/TPCDS-Queries)
  are completely pushed to Oracle instance.
- Deployable as a spark extension jar for Spark 3 environments.
- [Language integration beyond SQL](https://github.com/oracle/spark-oracle/wiki/Language-Integration)
  and [DML](https://github.com/oracle/spark-oracle/wiki/Write-Path-Flow) support.

See [Project Wiki](https://github.com/oracle/spark-oracle/wiki/home) for complete documenation.


## Installation

Spark_on_Oracle can be deployed on any Spark 3.1 or above environment.
See the [Quick Start Guide](https://github.com/oracle/spark-oracle/wiki/Quick-Start-Guide)

## Documentation

See the [wiki](https://github.com/oracle/spark-oracle/wiki/home)


## Examples

The [Demo script](https://github.com/oracle/spark-oracle/wiki/Demo) walks you
through the features of the library.

## Help

Please file Github issues.

## Contributing

<!-- If your project has specific contribution requirements, update the
    CONTRIBUTING.md file to ensure those requirements are clearly explained. -->

This project welcomes contributions from the community. Before submitting a pull
request, please [review our contribution guide](./CONTRIBUTING.md).

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security
vulnerability disclosure process.

## License

<!-- The correct copyright notice format for both documentation and software
    is "Copyright (c) [year,] year Oracle and/or its affiliates."
    You must include the year the content was first released (on any platform) and
    the most recent year in which it was revised. -->

Copyright (c) 2021 Oracle and/or its affiliates.

<!-- Replace this statement if your project is not licensed under the UPL -->

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.
# spark-oracle
- See [Project Wiki](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/home) for details
- See [Quick Start](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/Quick-Start-Guide) 
  or [Developer Env.](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/Developer-Env)
  to get started.
  
## Current Status
- Good support for SQL pushdown to the extent that more than 95(of 99) TPCDS queries
  are completely pushed to Oracle instance.
- Available as an extension jar for Spark 3 environments.  
- Language integration beyond SQL and DML are in active development.
- See [Demo](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/Demo) for a step-by-step
  guide on how to try many features.

## Background and Motivation
- Currently Data Lakes comprising of Oracle Data Warehouse and Apache Spark
  - Have **separate Data Catalogs**. Even if they access the same data on Object Store.
  - Applications build entirely on Spark, have to **compensate for gaps in Data Management.**
  - Applications that federate across Spark and Oracle usually suffer from
    **inefficient data movement.**
  - Operating Spark clusters are expensive, because of lack of administration tooling
    and gaps in data management. **So price-performance advantages of Spark are overstated.**

![current deployments](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/uploads/currentDeploymentDrawbacks.png)
    
## Spark on Oracle
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
  
![spark on oracle](https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/uploads/spark-on-oracle.png)

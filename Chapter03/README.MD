# Chapter 3: Data Ingestion Strategies for Azure Databricks

## Summary

In this chapter, we explore batch data ingestion strategies for Azure Databricks. We cover ingesting data from Azure storage (ADLS/Blob), connecting to relational databases via JDBC, consuming REST APIs, and using Azure Data Factory for data movement. The chapter focuses on practical patterns with working examples that demonstrate authentication, file format handling, incremental loading, and writing to Delta Lake. For streaming ingestion scenarios, see Chapter 5.

## Chapter Structure (6 Main Sections)

### 1. Understanding Batch Ingestion
**Goal**: Define batch ingestion and when to use it

### 2. Ingesting Data from Azure Storage (ADLS & Blob Storage)
**Goal**: Master cloud storage ingestion with authentication and file formats

### 3. Using Azure Data Factory for Ingestion
**Goal**: Understand ADF's role in data movement

### 4. Connecting to Relational Databases
**Goal**: Learn JDBC connectivity patterns

### 5. Ingesting Data from REST APIs
**Goal**: Handle API-based data sources

### 6. Other Data Sources
**Goal**: Cover Cosmos DB and SFTP integration

## Hands-on

1. `01_adls_batch_ingestion.py` - This exercise demonstrates batch ingestion from Azure Data Lake Storage Gen2, covering service principal authentication, reading multiple file formats (CSV, JSON, Parquet), joining data from different sources, and writing to partitioned Delta Lake tables with Unity Catalog integration.

2. `02_azure_sql_full_load.py` - This exercise shows how to perform a full table load from Azure SQL Database using JDBC, reading multiple related tables (customers, orders), adding ingestion metadata, and creating Unity Catalog managed tables with analytical views.

3. `03_azure_sql_incremental_load.py` - This exercise demonstrates efficient incremental loading using watermark-based change detection, including control table management, Delta Lake MERGE operations for upserts, and automatic watermark updates after successful processing.

4. `04_rest_api_ingestion.py` - This exercise shows how to ingest data from REST APIs using JSONPlaceholder (free public API), covering HTTP requests, flattening nested JSON structures, converting via pandas to Spark DataFrames, and creating enriched views by joining data from multiple API endpoints.


# Chapter 7: Automating Data Pipelines with Lakeflow Spark Declarative Pipelines

## Summary

In this chapter, we explore how to build automated, production-ready data pipelines using Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables). We cover the declarative approach to pipeline development, implementing data quality expectations, and understanding streaming aggregations with watermarks. The chapter demonstrates a complete medallion architecture pipeline (Bronze → Silver → Gold) with Unity Catalog integration, showing how Lakeflow automatically handles dependency management, incremental processing, and data quality monitoring.

## Chapter Structure (4 Main Sections)

### 1. Introduction to Lakeflow Spark Declarative Pipelines
**Goal**: Understand the declarative approach and key benefits over traditional pipelines

### 2. Building Your First Lakeflow Pipeline
**Goal**: Create a complete Bronze-Silver-Gold pipeline with working code

### 3. Managing Data Quality with Expectations
**Goal**: Implement and monitor data quality rules with expectations

### 4. Optimizing Lakeflow Pipeline Performance
**Goal**: Understand performance considerations and monitoring techniques

## Hands-on

1. `01_spark_declarative_pipeline.py` - This exercise demonstrates a complete Lakeflow pipeline covering: Auto Loader ingestion from Unity Catalog Volumes (Bronze layer), data cleaning with quality expectations including both drop rules and monitoring rules (Silver layer), and streaming aggregations with watermarks and time windows for geographic sales analytics (Gold layer). The pipeline shows 52 orders dropped due to invalid status and 11 customers flagged with signup date warnings, illustrating both enforcement and monitoring patterns. Includes stream-to-static joins, approximate distinct counts for streaming compatibility, and automatic dependency management across all layers.

## Data Setup

The pipeline uses sample data files (`orders.csv` and `customers.csv`) from the Chapter 2 repository folder. Upload these files to a Unity Catalog Volume at `/Volumes/cat_dev/vol_chapter7/sdp/` before running the pipeline. The setup section in the chapter provides detailed instructions for creating the Volume and verifying file uploads.

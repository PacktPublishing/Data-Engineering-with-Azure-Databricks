# Chapter 5: Building Real-Time Data Pipelines

## Summary

In this chapter, we will explore the fundamentals of data streaming and how to build real-time data pipelines using Azure Databricks. We will start by understanding what streaming data is and how it differs from traditional batch processing, examining common use cases and when to choose streaming over batch approaches. We will then dive into the different types of streaming - real-time versus near real-time - and learn how to select the right approach for your specific needs. Next, we will explore Apache Spark Structured Streaming capabilities and how Azure Databricks enhances these features with cloud-native integrations, auto-scaling, and enterprise security, including the powerful Autoloader feature for file-based streaming. We will walk through building your first streaming pipeline with hands-on examples using Azure Event Hubs, Blob Storage, and Autoloader for automatic file processing. Finally, we will cover advanced streaming concepts including stateful processing, handling late-arriving data, fault tolerance, and performance optimization techniques for production-ready streaming applications. 

## Chapter Structure (5 Main Sections)

### 1. What is Data Streaming?
**Goal**: Build foundation understanding of streaming concepts

### 2. Types of Streaming: Real-Time vs Near Real-Time
**Goal**: Help readers choose the right streaming approach

### 3. Azure Databricks and Spark Streaming Capabilities
**Goal**: Show what Databricks can do for streaming

### 4. Building Your First Streaming Pipeline
**Goal**: Hands-on example with step-by-step instructions

### 5. Advanced Streaming Concepts
**Goal**: Cover production-ready features

## Hands-on

1. `01_streaming_pipeline_with_event_hub.py` - This exercise shows how to build a basic streaming pipeline that reads data from Azure Event Hubs. It covers connecting to Event Hubs using the Spark connector, parsing JSON arrays containing IoT sensor data (device ID, temperature, humidity, pressure, battery, location), and writing the parsed events to a Delta table with proper schema handling and checkpointing.
2. `02_streaming_pipeline_with_autoloader.py` - This exercise demonstrates using Databricks Auto Loader to read JSON files from Azure Blob Storage in a streaming manner. It covers how Auto Loader automatically handles schema inference, schema evolution, tracks processed files, and ingests new files as they arrive in the storage container, while also showing how to use the rescuedDataColumn feature to capture unparsed data for troubleshooting.
3. `03_stateful_streaming_pipeline.py` - This exercise demonstrates advanced stateful streaming operations using watermarks and time-window aggregations. It covers two key patterns: (1) time-window aggregations that calculate statistics (avg, min, max, count) for each device over 5-minute windows, and (2) real-time alert generation that detects threshold violations (critical temperature/battery levels) and writes alerts with severity levels to a separate Delta table for monitoring and notifications.

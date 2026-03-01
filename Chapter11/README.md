# Chapter 11: Security, Compliance, and Data Governance

## Summary

In this chapter, we explore how to secure an Azure Databricks environment, implement fine-grained access controls, and establish compliance and governance practices. We cover infrastructure security with VNet injection and encryption, column masking and row-level security with Unity Catalog, auditing and monitoring through system tables and Azure diagnostic settings, compliance patterns for GDPR and HIPAA, automated data lineage tracking, and secure data sharing with Delta Sharing. The chapter demonstrates practical implementations using SQL functions for masking and filtering, audit queries against system tables, erasure workflows with Delta Lake, and lineage queries for impact analysis.

## Chapter Structure (6 Main Sections)

### 1. Securing the Databricks Environment
**Goal**: Harden the workspace with network security, encryption, and workspace-level settings

### 2. Fine-Grained Access Control with Unity Catalog
**Goal**: Implement column masks, row filters, and dynamic views for sensitive data protection

### 3. Auditing and Monitoring Data Access
**Goal**: Query system tables for audit trails and set up proactive monitoring with alerts

### 4. Ensuring Compliance with Regulatory Standards
**Goal**: Implement GDPR erasure workflows, data classification with tags, and HIPAA safeguards

### 5. Data Lineage and Impact Analysis
**Goal**: Use Unity Catalog lineage to trace data flows and assess schema change impact

### 6. Secure Data Sharing with Delta Sharing
**Goal**: Share data securely across workspaces and with external organizations

## Hands-on

1. `01_column_masks_and_row_filters.sql` - This exercise demonstrates fine-grained access control covering: creating masking functions for email, phone, and monetary values in a dedicated security schema, applying column masks and row filters to a customer analytics table, and tagging columns with PII classification metadata. The example shows how the same table serves different audiences—analysts see masked values and region-filtered rows, while authorized groups see full data—without any application code changes.

## Data Setup

The exercises use the same sample data files (`orders.csv` and `customers.csv`) from the book's GitHub repository. These should already be loaded into Unity Catalog tables from earlier chapters. The fine-grained access control example (Exercise 1) requires at least two account-level groups (such as `pii_viewers` and `analysts`) to demonstrate differential access.
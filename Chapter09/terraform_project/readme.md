# Azure Databricks Unity Catalog with Terraform

This project deploys a full Unity Catalog environment on Azure Databricks. Each environment (dev, staging) gets its own storage, connectors, metastore, and catalog. Nothing is shared between environments.

## Before You Start

You need three things:

1. **Terraform** installed on your machine.
2. **Azure CLI** installed and logged in with `az login`.
3. **Databricks account admin** access. A current admin can grant this to you, or claim it as the first Azure AD Global Admin at https://accounts.azuredatabricks.net.

Each environment needs its own Databricks workspace. Create the workspace in Azure before running this project.

## Two Values to Collect

| Value | Where to Find It |
|---|---|
| `account_id` | Databricks account console, top-right corner or Settings page |
| `databricks_resource_id` | Azure Portal, your Databricks workspace, Overview page, JSON View link, copy Resource ID |

The resource ID looks like this:
`/subscriptions/xxxx/resourceGroups/my-rg/providers/Microsoft.Databricks/workspaces/my-ws`

## Project Files

```
main.tf                  required providers
locals.tf                data sources and computed values
providers.tf             azurerm and databricks provider configs
variables.tf             three inputs: environment, account_id, databricks_resource_id
storage.tf               storage account and two containers
connectors.tf            access connector, role assignment, storage credential
external_locations.tf    external location for catalog storage
metastore.tf             metastore creation and workspace attachment
catalog.tf               environment catalog
outputs.tf               outputs
dev.tfvars               values for dev environment
staging.tfvars           values for staging environment
```

## What Gets Created

The project creates these Azure and Databricks resources per environment:

1. A managed identity access connector.
2. An ADLS Gen2 storage account with hierarchical namespace.
3. Two storage containers: one for the metastore root, one for the catalog root.
4. A role assignment granting the managed identity Storage Blob Data Contributor access.
5. A Unity Catalog metastore attached to your workspace.
6. A storage credential and external location.
7. A catalog named after the environment (e.g. `dev_catalog`, `staging_catalog`).

All resource names include the environment name. Dev and staging resources never collide.

## Steps to Deploy

Open the tfvars file for your environment. Replace the placeholder values with your real ones.

Example `dev.tfvars`:
```hcl
environment            = "dev"
account_id             = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
databricks_resource_id = "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/workspaces/<dev-workspace>"
```

Run these commands:

```bash
terraform init
```

Create a workspace for your environment and deploy:

```bash
terraform workspace new dev
terraform apply -var-file=dev.tfvars
```

To deploy staging, create a second workspace and point to the staging tfvars:

```bash
terraform workspace new staging
terraform apply -var-file=staging.tfvars
```

## Switching Between Environments

```bash
terraform workspace select dev
terraform plan -var-file=dev.tfvars
```

```bash
terraform workspace select staging
terraform plan -var-file=staging.tfvars
```

## Tearing Down an Environment

```bash
terraform workspace select dev
terraform destroy -var-file=dev.tfvars
```

**Important:** `terraform destroy` only removes resources managed by Terraform. The Databricks workspaces were created manually in the Azure Portal and must be deleted manually as well. Delete them from the Azure Portal when you no longer need them.

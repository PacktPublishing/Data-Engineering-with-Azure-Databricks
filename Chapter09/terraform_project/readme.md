# Azure Databricks Unity Catalog with Terraform

This project deploys a full Unity Catalog environment on Azure Databricks. Each environment (dev, staging) gets its own storage, connectors, metastore, and catalog. Nothing is shared between environments.

## Before You Start

You need three things:

1. **Terraform** installed on your machine.
2. **Azure CLI** installed and logged in with `az login`.
3. **Databricks account admin** access. A current admin can grant this to you, or claim it as the first Azure AD Global Admin at https://accounts.azuredatabricks.net.

Each environment needs its own Databricks workspace. Create the workspace in Azure before running this project.

## Values to Collect

| Value        | Where to Find It                                                               |
| ------------ | ------------------------------------------------------------------------------ |
| `account_id` | Databricks account console, top-right corner or Settings page                  |
| `region`     | Azure region where your workspaces are deployed (e.g. `eastus`)                |
| `workspaces` | Azure Portal, each Databricks workspace, Overview, JSON View, copy Resource ID |
| `catalogs`   | Catalog names you want per environment                                         |

The workspace resource ID looks like this:
`/subscriptions/xxxx/resourceGroups/my-rg/providers/Microsoft.Databricks/workspaces/my-ws`

## Project Files

```
main.tf                    required providers
locals.tf                  data sources and computed values
providers.tf               azurerm and databricks provider configs
variables.tf               input variables
storage.tf                 storage account and two containers
connectors.tf              access connector, role assignment, storage credential
external_locations.tf      external location for catalog storage
metastore.tf               metastore creation and workspace attachment
catalog.tf                 environment catalog
outputs.tf                 outputs
terraform.tfvars.example   example variable values (copy to terraform.tfvars)
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

Copy the example file and fill in your values:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Initialize Terraform:

```bash
terraform init
```

Create a workspace for the first environment and deploy with `create_metastore=true`:

```bash
terraform workspace select -or-create dev
terraform plan -var="environment=dev" -var="create_metastore=true"
terraform apply -var='environment=dev' -var='create_metastore=true'
```

To deploy staging, create a second workspace. The metastore already exists so `create_metastore` defaults to `false`:

```bash
terraform workspace select -or-create staging
terraform plan -var="environment=staging"
terraform apply -var='environment=staging'
```

## Tearing Down an Environment

Destroy staging first (no metastore dependency), then dev:

```bash
terraform workspace select staging
terraform destroy -var='environment=staging'
```

```bash
terraform workspace select dev
terraform destroy -var='environment=dev' -var='create_metastore=true'
```

**Important:** `terraform destroy` only removes resources managed by Terraform. The Databricks workspaces were created manually in the Azure Portal and must be deleted manually as well. Delete them from the Azure Portal when you no longer need them.

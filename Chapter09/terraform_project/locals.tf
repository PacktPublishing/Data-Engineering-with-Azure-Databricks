data "azurerm_client_config" "current" {
}

data "azurerm_resource_group" "this" {
  name = local.resource_group
}

data "azurerm_databricks_workspace" "this" {
  name                = local.databricks_workspace_name
  resource_group_name = local.resource_group
}

locals {
  resource_regex            = "(?i)subscriptions/(.+)/resourceGroups/(.+)/providers/Microsoft.Databricks/workspaces/(.+)"
  subscription_id           = regex(local.resource_regex, var.workspaces[var.environment])[0]
  resource_group            = regex(local.resource_regex, var.workspaces[var.environment])[1]
  databricks_workspace_name = regex(local.resource_regex, var.workspaces[var.environment])[2]
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  databricks_workspace_host = data.azurerm_databricks_workspace.this.workspace_url
  databricks_workspace_id   = data.azurerm_databricks_workspace.this.workspace_id
  metastore_id              = var.create_metastore ? databricks_metastore.this[0].id : data.databricks_metastore.existing[0].metastore_id
  catalogs                  = var.catalogs[var.environment]
}

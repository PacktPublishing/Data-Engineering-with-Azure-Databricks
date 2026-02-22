provider "azurerm" {
  subscription_id = local.subscription_id
  features {}
}

// Provider for databricks workspace
provider "databricks" {
  host = local.databricks_workspace_host
}

// Provider for databricks account
provider "databricks" {
  alias           = "azure_account"
  host            = "https://accounts.azuredatabricks.net"
  account_id      = var.account_id
  azure_tenant_id = local.tenant_id
  auth_type       = "azure-cli"
}

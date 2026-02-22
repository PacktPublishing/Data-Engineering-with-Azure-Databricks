// Unity catalog metastore (one per region, shared across environments)
resource "databricks_metastore" "this" {
  provider      = databricks.azure_account
  count         = var.create_metastore ? 1 : 0
  name          = "${var.region}-metastore"
  region        = var.region
  force_destroy = true
}

// Look up existing metastore when not creating
data "databricks_metastore" "existing" {
  count    = var.create_metastore ? 0 : 1
  provider = databricks.azure_account
  name     = "${var.region}-metastore"
}

// Assign managed identity to metastore (only when creating)
resource "databricks_metastore_data_access" "first" {
  provider     = databricks.azure_account
  count        = var.create_metastore ? 1 : 0
  metastore_id = local.metastore_id
  name         = "metastore-key"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity.id
  }
  is_default = true
}

// Attach the databricks workspace to the metastore
resource "databricks_metastore_assignment" "this" {
  workspace_id = local.databricks_workspace_id
  metastore_id = local.metastore_id
}

resource "databricks_default_namespace_setting" "this" {
  namespace {
    value = "main"
  }
}

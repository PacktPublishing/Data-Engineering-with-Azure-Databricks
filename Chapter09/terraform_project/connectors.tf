// Managed identity access connector for Unity Catalog
resource "azurerm_databricks_access_connector" "unity" {
  name                = "dbac-uc-${var.environment}-001"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.region
  identity {
    type = "SystemAssigned"
  }
}

// Grant managed identity access to the storage account
resource "azurerm_role_assignment" "mi_data_contributor" {
  scope                = azurerm_storage_account.unity_catalog.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
}

// Storage credential wrapping the managed identity
resource "databricks_storage_credential" "external_mi" {
  name = "${var.environment}_uc_credential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity.id
  }
  comment    = "Storage credential for ${var.environment} Unity Catalog"
  depends_on = [databricks_metastore_assignment.this]
}

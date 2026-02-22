// Storage account for Unity Catalog (one per environment)
resource "azurerm_storage_account" "unity_catalog" {
  name                     = "dlsuc${var.environment}001"
  resource_group_name      = data.azurerm_resource_group.this.name
  location                 = var.region
  tags                     = data.azurerm_resource_group.this.tags
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

// One container per catalog
resource "azurerm_storage_container" "catalog" {
  for_each              = toset(local.catalogs)
  name                  = replace(each.key, "_", "-")
  storage_account_id    = azurerm_storage_account.unity_catalog.id
  container_access_type = "private"
}

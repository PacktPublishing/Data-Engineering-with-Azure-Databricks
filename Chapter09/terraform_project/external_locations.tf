// One external location per catalog
resource "databricks_external_location" "catalog" {
  for_each = toset(local.catalogs)
  name     = "${each.key}_location"
  url = format("abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.catalog[each.key].name,
    azurerm_storage_account.unity_catalog.name)
  credential_name = databricks_storage_credential.external_mi.id
  comment         = "External location for ${each.key} catalog"
}

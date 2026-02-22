// One catalog per entry
resource "databricks_catalog" "this" {
  for_each     = toset(local.catalogs)
  metastore_id = local.metastore_id
  name         = each.key
  comment      = "Catalog ${each.key}"
  storage_root = databricks_external_location.catalog[each.key].url
  properties = {
    purpose = var.environment
  }
  depends_on = [databricks_external_location.catalog]
}

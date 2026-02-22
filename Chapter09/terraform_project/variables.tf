variable "environment" {
  description = "Environment name (e.g. dev, staging)"
  type        = string
}
variable "account_id" {
  description = "Azure databricks account id"
}
variable "region" {
  description = "Azure region for all resources (e.g. westus2, eastus)"
  type        = string
}
variable "workspaces" {
  description = "Map of environment name to Azure Databricks workspace resource ID"
  type        = map(string)
}
variable "create_metastore" {
  description = "Whether to create a new metastore (true for first environment, false for subsequent)"
  type        = bool
  default     = false
}
variable "catalogs" {
  description = "Map of environment name to list of catalog names"
  type        = map(list(string))
}

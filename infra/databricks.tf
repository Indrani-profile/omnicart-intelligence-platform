resource "azurerm_databricks_workspace" "omnicart" {
  name                        = "omnicart-databricks"
  resource_group_name         = azurerm_resource_group.datalake.name
  location                    = azurerm_resource_group.datalake.location
  sku                         = "trial"
  managed_resource_group_name = "omnicart-databricks-managed-rg"

  tags = local.tags
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.omnicart.workspace_url
}

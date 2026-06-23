data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "omnicart" {
  name                       = "omnicart-kv"
  resource_group_name        = azurerm_resource_group.datalake.name
  location                   = azurerm_resource_group.datalake.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  tags = local.tags
}

resource "azurerm_key_vault_access_policy" "current_user" {
  key_vault_id = azurerm_key_vault.omnicart.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
}

resource "azurerm_key_vault_secret" "adls_connection_string" {
  name         = "adls-connection-string"
  value        = azurerm_storage_account.datalake.primary_connection_string
  key_vault_id = azurerm_key_vault.omnicart.id

  depends_on = [azurerm_key_vault_access_policy.current_user]
}

resource "azurerm_key_vault_secret" "eventhubs_connection_string" {
  name         = "eventhubs-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.send_listen.primary_connection_string
  key_vault_id = azurerm_key_vault.omnicart.id

  depends_on = [azurerm_key_vault_access_policy.current_user]
}

resource "azurerm_key_vault_secret" "snowflake_password" {
  name         = "snowflake-password"
  value        = var.snowflake_password
  key_vault_id = azurerm_key_vault.omnicart.id

  depends_on = [azurerm_key_vault_access_policy.current_user]
}

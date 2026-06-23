resource "azurerm_eventhub_namespace" "omnicart" {
  name                = "omnicart-events"
  resource_group_name = azurerm_resource_group.datalake.name
  location            = azurerm_resource_group.datalake.location
  sku                 = "Standard"
  capacity            = 1

  tags = local.tags
}

resource "azurerm_eventhub" "order_events" {
  name                = "order-events"
  namespace_name      = azurerm_eventhub_namespace.omnicart.name
  resource_group_name = azurerm_resource_group.datalake.name
  partition_count     = 3
  message_retention   = 7
}

resource "azurerm_eventhub_consumer_group" "databricks_consumer" {
  name                = "databricks-consumer"
  namespace_name      = azurerm_eventhub_namespace.omnicart.name
  eventhub_name       = azurerm_eventhub.order_events.name
  resource_group_name = azurerm_resource_group.datalake.name
}

resource "azurerm_eventhub_namespace_authorization_rule" "send_listen" {
  name                = "send-listen-policy"
  namespace_name      = azurerm_eventhub_namespace.omnicart.name
  resource_group_name = azurerm_resource_group.datalake.name

  listen = true
  send   = true
  manage = false
}

output "eventhub_namespace_primary_connection_string" {
  description = "Primary connection string for the send+listen SAS policy on the Event Hubs namespace"
  value       = azurerm_eventhub_namespace_authorization_rule.send_listen.primary_connection_string
  sensitive   = true
}

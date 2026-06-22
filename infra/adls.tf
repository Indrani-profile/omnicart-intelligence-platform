terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  tags = {
    project     = var.project_name
    environment = "dev"
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "datalake" {
  name     = var.resource_group_name
  location = var.location

  tags = local.tags
}

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.datalake.name
  location                 = azurerm_resource_group.datalake.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  tags = local.tags
}

resource "azurerm_storage_container" "containers" {
  for_each = toset(["raw", "bronze", "silver", "gold"])

  name                  = each.value
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "datalake" {
  storage_account_id = azurerm_storage_account.datalake.id

  rule {
    name    = "raw-delete-after-90-days"
    enabled = true

    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["raw/"]
    }

    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 90
      }
    }
  }

  rule {
    name    = "bronze-cool-after-30-days"
    enabled = true

    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["bronze/"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }
}

output "storage_account_primary_connection_string" {
  description = "Primary connection string for the ADLS Gen2 storage account"
  value       = azurerm_storage_account.datalake.primary_connection_string
  sensitive   = true
}

output "storage_account_primary_access_key" {
  description = "Primary access key for the ADLS Gen2 storage account"
  value       = azurerm_storage_account.datalake.primary_access_key
  sensitive   = true
}

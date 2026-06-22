variable "resource_group_name" {
  description = "Name of the Azure resource group that holds the data lake resources"
  type        = string
  default     = "omnicart-data-rg"
}

variable "location" {
  description = "Azure region for the data lake resources"
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account (must be globally unique, lowercase alphanumeric, 3-24 chars)"
  type        = string
  default     = "omnicartdatalake"
}

variable "project_name" {
  description = "Project name used for tagging resources"
  type        = string
  default     = "omnicart"
}

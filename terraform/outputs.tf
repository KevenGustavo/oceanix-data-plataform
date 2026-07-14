output "resource_group_name" {
  description = "Nome do Resource Group criado"
  value       = azurerm_resource_group.rg.name
}

output "datalake_name" {
  description = "Nome da Storage Account (Data Lake)"
  value       = azurerm_storage_account.datalake.name
}
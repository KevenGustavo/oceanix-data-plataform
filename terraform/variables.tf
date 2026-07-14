variable "resource_group_name" {
  type        = string
  description = "Nome base do Resource Group na Azure"
}

variable "location" {
  type        = string
  description = "Região da Azure onde os recursos serão criados"
  default     = "East US"
}

variable "storage_account_name" {
  type        = string
  description = "Nome único da Storage Account (Data Lake Gen2)"
}

variable "environment" {
  type        = string
  description = "Ambiente de implantação (ex: dev, qa, prod)"
  default     = "dev"
}
from airflow.sdk import Asset

silver_gfw_asset = Asset("azure://silver/gfw_flattened")
silver_weather_asset = Asset("azure://silver/weather_flattened")
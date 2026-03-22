# DEPRECATED: This Trino catalog template is no longer used.
# Catalogs are now managed by AWS Glue Data Catalog (see terraform/modules/trino/main.tf).
# Retained for reference during migration. Do not modify.
connector.name=mysql
connection-url=jdbc:mysql://${mysql_host}:${mysql_port}/${mysql_database}
connection-user=${mysql_user}
connection-password=${mysql_password}
case-insensitive-name-matching=true
mysql.auto-reconnect=true
mysql.max-reconnects=3
mysql.connection-timeout=10s

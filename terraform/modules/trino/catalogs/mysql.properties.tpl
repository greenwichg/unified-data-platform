connector.name=mysql
connection-url=jdbc:mysql://${mysql_host}:${mysql_port}/${mysql_database}
connection-user=${mysql_user}
connection-password=${mysql_password}
case-insensitive-name-matching=true
mysql.auto-reconnect=true
mysql.max-reconnects=3
mysql.connection-timeout=10s

# DEPRECATED: This Trino catalog template is no longer used.
# Catalogs are now managed by AWS Glue Data Catalog (see terraform/modules/trino/main.tf).
# Retained for reference during migration. Do not modify.
connector.name=hive
hive.metastore.uri=thrift://${hive_metastore_host}:${hive_metastore_port}
hive.config.resources=/etc/trino/core-site.xml
hive.allow-drop-table=true
hive.allow-rename-table=true
hive.allow-add-column=true
hive.allow-drop-column=true
hive.allow-rename-column=true
hive.non-managed-table-writes-enabled=true
hive.s3.path-style-access=false
hive.s3.ssl.enabled=true
hive.s3.sse.enabled=true
hive.s3.endpoint=${s3_endpoint}
hive.parquet.use-column-names=true
hive.orc.use-column-names=true
hive.storage-format=ORC
hive.compression-codec=ZSTD
hive.recursive-directories=true

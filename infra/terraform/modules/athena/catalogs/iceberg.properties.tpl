# DEPRECATED: This Trino catalog template is no longer used.
# Catalogs are now managed by AWS Glue Data Catalog (see terraform/modules/athena/main.tf).
# Retained for reference during migration. Do not modify.
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://${hive_metastore_host}:${hive_metastore_port}
iceberg.file-format=ORC
iceberg.compression-codec=ZSTD
iceberg.max-partitions-per-writer=1000
iceberg.target-max-file-size=1073741824
iceberg.expire_snapshots.min-retention=7d
iceberg.delete-orphan-files.min-retention=7d
hive.s3.path-style-access=false
hive.s3.ssl.enabled=true
hive.s3.sse.enabled=true
hive.s3.endpoint=${s3_endpoint}

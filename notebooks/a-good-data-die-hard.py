# Databricks notebook source
# MAGIC %python
# MAGIC import urllib.request
# MAGIC 
# MAGIC (csv_filename, headers) = urllib.request.urlretrieve("https://cve.mitre.org/data/downloads/allitems.csv","/databricks/driver/allitems.csv")
# MAGIC dbutils.fs.mv("file:%s" % csv_filename, "dbfs:/Filestore/tables/allitems.csv")
# MAGIC 
# MAGIC data = spark.read.csv("dbfs:/FileStore/tables/allitems.csv", header="true", inferSchema="true")
# MAGIC data.cache()
# MAGIC data = data.dropna()
# MAGIC 
# MAGIC data.take(1)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace, col, lower
year_column = data.withColumn('Year', regexp_extract(col('Name'), '^CVE-([0-9]*)-.*', 1))
result = year_column.withColumn('OperatingSystem', regexp_extract(col('Description'), '.*(?i)(Android|Windows|BSD|Linux|macOS|OSX|OS X|iOS).*', 1)).withColumn('OperatingSystem', lower(col('OperatingSystem'))).withColumn('OperatingSystem', regexp_replace('OperatingSystem', 'macos|osx|os x', 'macos'))
result.createOrReplaceTempView("os_column")

# COMMAND ----------

configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
           "fs.adl.oauth2.client.id": "be0dc5cd-38a7-4e65-890f-f0f4491179eb",
           "fs.adl.oauth2.credential": dbutils.secrets.get(scope = "cve", key = "sp-key"),
           "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/6b4c2372-ae97-4555-af13-7fc2b57dfe28/oauth2/token"}

# Check if the ADL store is mounted already, and if not, mount it
if not any(mount.mountPoint == '/mnt/databricks-output-data-lake-gen1' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = "adl://adlaagddh.azuredatalakestore.net/",
    mount_point = "/mnt/databricks-output-data-lake-gen1",
    extra_configs = configs
  )

result.write.format("parquet").mode("overwrite").save("/mnt/databricks-output-data-lake-gen1/mitre-cve.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM os_column WHERE NULLIF(OperatingSystem, '') IS NOT NULL LIMIT 1000;
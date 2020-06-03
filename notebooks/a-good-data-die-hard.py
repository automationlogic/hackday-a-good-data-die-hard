# Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets/samples/docs/"))
display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

# MAGIC %python
# MAGIC import urllib.request
# MAGIC 
# MAGIC # (csv_filename, headers) = urllib.request.urlretrieve("https://cve.mitre.org/data/downloads/allitems.csv")
# MAGIC # print(csv_filename)
# MAGIC # dbutils.fs.cp("file:/databricks/driver%s" % csv_filename, "dbfs:/datasets/allitems.csv")
# MAGIC data = spark.read.csv("dbfs:/FileStore/tables/allitems.csv", header="true", inferSchema="true")
# MAGIC data.cache()
# MAGIC data = data.dropna()
# MAGIC 
# MAGIC data.take(1)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col

result = data.withColumn('OS', regexp_extract(col('Description'), '.*(Android|Windows|BSD|Linux|MacOS).*', 1))
result.createOrReplaceTempView("os_column")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM os_column WHERE LENGTH(OS) > 1;

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://databricks-output@datahardstorage.blob.core.windows.net/",
  mount_point = "/mnt/databricks-output",
  extra_configs = {"fs.azure.account.key.datahardstorage.blob.core.windows.net":dbutils.secrets.get(scope = "cve", key = "datahardstorage-access-key")})

result.write.format("parquet").save("/mnt/databricks-output/mitre-cve.parquet")
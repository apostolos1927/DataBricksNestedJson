-- Databricks notebook source
-- DBTITLE 0,--i18n-daae326c-e59e-429b-b135-5662566b6c34
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Complex Transformations
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("json").option("multiline","true").load("dbfs:/FileStore/tables/example.json")
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode('overwrite').saveAsTable("testdata")

-- COMMAND ----------

SELECT * FROM testdata

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6a0cd9e-3bdc-463a-879a-5551fa9a8449
-- MAGIC %md
-- MAGIC
-- MAGIC ### Work with Nested Data
-- MAGIC
-- MAGIC - Use **`.`** syntax in queries to access subfields in struct types

-- COMMAND ----------

SELECT medications.aceInhibitors.name FROM testdata

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df
-- MAGIC     .where("medications.aceInhibitors.name = 'lisinopril'")
-- MAGIC     .select(df.medications.aceInhibitors.dose)
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-914b04cd-a1c1-4a91-aea3-ecd87714ea7d
-- MAGIC %md
-- MAGIC - **`schema_of_json()`** returns the schema derived from an example JSON string.
-- MAGIC - **`from_json()`** parses a column containing a JSON string into a struct type using the specified schema.

-- COMMAND ----------

SELECT schema_of_json('{"aceInhibitors": {"dose": "1 tab", "name": "lisinopril", "pillCount": "#90", "refills": "Refill 3", "route": "PO", "sig": "daily", "strength": "10 mg Tab"}, "antianginal": {"dose": "1 tab", "name": "nitroglycerin", "pillCount": "#30", "refills": "Refill 1", "route": "SL", "sig": "q15min PRN", "strength": "0.4 mg Sublingual Tab"}, "anticoagulants": {"dose": "1 tab", "name": "warfarin sodium", "pillCount": "#90", "refills": "Refill 3", "route": "PO", "sig": "daily", "strength": "3 mg Tab"}}') AS schema

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW testdata_view 
AS SELECT T.* FROM (
  SELECT from_json(to_json(medications), 'STRUCT<aceInhibitors: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>, antianginal: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>, anticoagulants: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>>
') AS T 
FROM testdata);

SELECT * FROM testdata_view

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json,to_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC STRUCT<aceInhibitors: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>, antianginal: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>, anticoagulants: STRUCT<dose: STRING, name: STRING, pillCount: STRING, refills: STRING, route: STRING, sig: STRING, strength: STRING>>
-- MAGIC """
-- MAGIC testdataDF = (df
-- MAGIC     .select(from_json(to_json(df.medications), json_string).alias("T"))
-- MAGIC     .select("T.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(testdataDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ca54e9c-dcb7-4177-99ab-77377ce8d899
-- MAGIC %md
-- MAGIC ### Manipulate Arrays
-- MAGIC - **`explode()`** separates the elements of an array into multiple rows; this creates a new row for each element.
-- MAGIC - **`size()`** provides a count for the number of elements in an array for each row.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_view AS
SELECT *, explode(labs) AS item
FROM testdata;

SELECT * FROM exploded_view WHERE size(labs) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC test_data_explodedDF = (df
-- MAGIC     .withColumn("item", explode("labs"))
-- MAGIC )
-- MAGIC
-- MAGIC display(test_data_explodedDF.where(size("labs") > 2))

-- COMMAND ----------

-- DBTITLE 0,--i18n-0810444d-1ce9-4cb7-9ba9-f4596e84d895
-- MAGIC %md
-- MAGIC - **`collect_set()`** collects unique values for a field, including fields within arrays.
-- MAGIC - **`flatten()`** combines multiple arrays into a single array.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [('James','Java'),
-- MAGIC   ('James','Python'),
-- MAGIC   ('James','Python'),
-- MAGIC   ('Anna','PHP'),
-- MAGIC   ('Anna','Javascript'),
-- MAGIC   ('Maria','Java'),
-- MAGIC   ('Maria','C++'),
-- MAGIC   ('James','Scala'),
-- MAGIC   ('Anna','PHP'),
-- MAGIC   ('Anna','HTML')
-- MAGIC ]
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data,schema=["name","languages"])
-- MAGIC df.createOrReplaceTempView('test123')

-- COMMAND ----------

SELECT name,
  collect_set(languages) AS languages_agg
FROM test123
GROUP BY name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import  collect_set
-- MAGIC
-- MAGIC display(df
-- MAGIC     .groupby("name")
-- MAGIC     .agg(collect_set("languages").alias("languages_agg"))
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **`flatten()`** combines multiple arrays into a single array.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import flatten
-- MAGIC df3 = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],)], ['col1'])
-- MAGIC df3.createOrReplaceTempView('temp_data')
-- MAGIC df3.display()
-- MAGIC df3.select(flatten(df3.col1).alias('flattened_data')).show()

-- COMMAND ----------

SELECT flatten(col1) as flattened_data FROM temp_data

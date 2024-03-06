# Databricks notebook source
# DBTITLE 1,mount ufo-data
dbutils.fs.mount(
    source='wasbs://ufo-data@moviesdataset.blob.core.windows.net',
    mount_point='/mnt/ufo-data',
    extra_configs = {'fs.azure.account.key.moviesdataset.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)

# COMMAND ----------

# DBTITLE 1,View path info 
# MAGIC %fs
# MAGIC ls "/mnt/ufo-data"

# COMMAND ----------

# DBTITLE 1,Reads file format
ufo = spark.read.format("csv").option("header","true").load("/mnt/ufo-data/raw-data/ufo-sightings-transformed.csv")

# COMMAND ----------

# DBTITLE 1,Shows 15 results 
ufo.limit(15).show()

# COMMAND ----------

# DBTITLE 1,Prints scheme to show data type 
ufo.printSchema()

# COMMAND ----------

# DBTITLE 1,Import functions and types 
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

# DBTITLE 1,Changes Encounter_Duration to int
ufo = ufo.withColumn("Encounter_Duration", col("Encounter_Duration").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Cast Year to correct format as Int
ufo = ufo.withColumn("Year", col("Year").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Reads as a different format but doesn't change type like an input mask
from pyspark.sql import functions as F
ufo = ufo.withColumn("date_documented", F.to_date(ufo.date_documented))

# COMMAND ----------

ufo.write.mode("overwrite").option("header","true").csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# DBTITLE 1,SQL query that shows country and amount of UFO sightings per country 
#Register the DataFrame as a temporary table to run SQL queries
ufo.createOrReplaceTempView("ufo_table")

#Example query: Get the count of UFO sightings per country
result = spark.sql("""
    SELECT Country, COUNT(*) as CountOfSightings
    FROM ufo_table
    GROUP BY Country
    ORDER BY CountOfSightings DESC
""")
result.show()

# COMMAND ----------

# DBTITLE 1,Finds the average length of UFO encounters per UFO shape 
#Another query that Finds the average length of UFO encounters per UFO shape 
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")
result.show()

# COMMAND ----------

# DBTITLE 1,import desc and sql query that counts occurrences per country
from pyspark.sql.functions import desc
#Count occurence of each UFO sighting per country
longest_encounter_per_country = ufo.groupBy("Country").agg({"length_of_encounter_seconds": "max"}) \
    .withColumnRenamed("max(length_of_encounter_seconds)", "Longest_Encounter_Seconds") \
    .orderBy(desc("Longest_Encounter_Seconds"))




# COMMAND ----------

# DBTITLE 1,Shows data collected from above cell
# Display the longest UFO encounters per country as a bar chart
display(longest_encounter_per_country)

# COMMAND ----------

# DBTITLE 1,Shows latitude, longitude and country as map
#Filters out rows with missing or invalid lititude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

#Select columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

#Display the UFO sightings as a map
display(locations)

# COMMAND ----------

# DBTITLE 1,changes data from floating-point to DoubleType 
ufo = ufo.withColumn("latitude", col("latitude").cast(DoubleType()))
ufo = ufo.withColumn("longitude", col("longitude").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Allows you to save transformed DatafRame ufo to csv 
ufo.write.mode("overwrite").option("header", "true").csv("/mnt/ufo_data/transformed/ufo")

# COMMAND ----------

# DBTITLE 1,Edit of code above with cluster marks added 
# Filter out rows with missing or invalid latitude/longitude values
valid_location_data = ufo.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# Select necessary columns for mapping
locations = valid_location_data.select("latitude", "longitude", "Country")

# Display the UFO sightings on a map
display(locations)

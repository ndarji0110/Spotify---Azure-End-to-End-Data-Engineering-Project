# Databricks notebook source
# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

project_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

from utils.transformations import *

# COMMAND ----------

# DBTITLE 1,Cell 3
df_user = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@neelazureproject.dfs.core.windows.net/DimUser")
)

# COMMAND ----------

# DBTITLE 1,Cell 4
df_user = df_user.withColumn("user_name",upper(col("user_name")))


# COMMAND ----------

# DBTITLE 1,Cell 5
query = (
    df_user.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/checkpoint_v2"
    )
    .option(
        "path",
        "abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/testuser"
    )
    .trigger(availableNow=True)
    .toTable("spotify_cata.silver.testuser")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.silver.testuser

# COMMAND ----------

# DBTITLE 1,Cell 7
import time

checkpoint_path = f"abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/temp_table_checkpoint_{int(time.time())}"

query = (df_user.writeStream
.format("memory")
.option("checkpointLocation", checkpoint_path)
.queryName("temp_table")
.outputMode("append")
.trigger(availableNow=True)
.start())

time.sleep(5)

display(spark.sql("select * from temp_table"))

query.stop()



# COMMAND ----------

from utils.transformations import reusable

# COMMAND ----------

df_user_obj = reusable()


# COMMAND ----------

df_user = df_user_obj.dropcolumns(df_user,'_rescued_data')
df_user =  df_user.dropDuplicates(subset=['user_id'])

# COMMAND ----------

df_user.writeStream.format("delta")\
    .option("checkpointLocation","abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("path","abfss://silver@neelazureproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_artist = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@neelazureproject.dfs.core.windows.net/DimArtist/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@neelazureproject.dfs.core.windows.net/DimArtist")
)

# COMMAND ----------

df_art_obj = reusable()

df_artist = df_art_obj.dropcolumns(df_artist,'_rescued_data')
df_artist = df_artist.dropDuplicates(subset=['artist_id'])


# COMMAND ----------

df_artist.writeStream.format("delta")\
    .option("checkpointLocation","abfss://silver@neelazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("path","abfss://silver@neelazureproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

df_artist.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@neelazureproject.dfs.core.windows.net/DimTrack/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@neelazureproject.dfs.core.windows.net/DimTrack")
)

# COMMAND ----------

df_track = df_track.withColumn("durationFalg",when(col("duration_sec")<150,"low")\
                                            .when(col("duration_sec")>150,"medium")\
                                            .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))     


# COMMAND ----------

df_trk_obj = reusable()

df_track = df_trk_obj.dropcolumns(df_track,'_rescued_data')
df_track = df_track.dropDuplicates(subset=['track_id'])

# COMMAND ----------

df_track.writeStream.format("delta")\
    .option("checkpointLocation","abfss://silver@neelazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("path","abfss://silver@neelazureproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.silver.dimtrack

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@neelazureproject.dfs.core.windows.net/DimDate/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@neelazureproject.dfs.core.windows.net/DimDate")
)

# COMMAND ----------

df_trk_obj = reusable()

df_date = df_trk_obj.dropcolumns(df_time,'_rescued_data')
# df_track = df_track.dropDuplicates(subset=['track_id'])

# COMMAND ----------

df_date.writeStream.format("delta")\
    .option("checkpointLocation","abfss://silver@neelazureproject.dfs.core.windows.net/DimDate/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("path","abfss://silver@neelazureproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

df_fact = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@neelazureproject.dfs.core.windows.net/FactStream/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@neelazureproject.dfs.core.windows.net/FactStream")
)

# COMMAND ----------

df_trk_obj = reusable()

df_fact = df_trk_obj.dropcolumns(df_fact,'_rescued_data')
# df_track = df_track.dropDuplicates(subset=['track_id'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .option("checkpointLocation","abfss://silver@neelazureproject.dfs.core.windows.net/FactStream/checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .option("path","abfss://silver@neelazureproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")
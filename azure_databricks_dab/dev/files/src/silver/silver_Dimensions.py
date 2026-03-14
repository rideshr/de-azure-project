# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path = os.path.join(os.getcwd(), '..', '..')

sys.path.append(project_path)

from utils.transformations import Reusable

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim User AutoLoader

# COMMAND ----------



# COMMAND ----------
df_user = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/schema") \
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/_display_checkpoint")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------



# COMMAND ----------

df_user_obj = Reusable()
df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"]);

# COMMAND ----------

display(df_user, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/_display_checkpoint_2")

# COMMAND ----------

df_user = df_user.dropDuplicates(["user_id"]);

# COMMAND ----------

dbutils.fs.rm("abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/checkpoint", recurse=True)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@sadeazureproject.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimArtist AutoLoader

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/checkpoint") \
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_artist = df_artist.withColumn("artist_name", upper(col("artist_name"))) 

# COMMAND ----------

df_artist_obj = Reusable()
df_artist = df_artist_obj.dropColumns(df_artist, ["_rescued_data"]);

# COMMAND ----------

df_artist = df_artist.dropDuplicates(["artist_id"]);

# COMMAND ----------

display(df_artist, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/checkpoint")

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/data")

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimTrack AutoLoader

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/checkpoint") \
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/DimTrack")
df_track = df_track.withColumn("track_name", upper(col("track_name"))) 

df_track_obj = Reusable()
df_track = df_track_obj.dropColumns(df_track, ["_rescued_data"]);

df_track = df_track.dropDuplicates(["track_id"]);

display(df_track, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/checkpoint")

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/data")
    
df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@sadeazureproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimDate AutoLoader

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/checkpoint") \
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/Dimdate")
df_date = df_date.withColumn("date_name", upper(col("date_name"))) 

df_date_obj = Reusable()
df_date = df_date_obj.dropColumns(df_date, ["_rescued_data"]);

df_date = df_date.dropDuplicates(["date_id"]);

display(df_date, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/checkpoint")

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/data")

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@sadeazureproject.dfs.core.windows.net/Dimdate/data")\
    .toTable("spotify_catalog.silver.Dimdate")

# COMMAND ----------

# MAGIC %md
# MAGIC # FactStream Autoloader

# COMMAND ----------

df_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/checkpoint") \
    .load("abfss://bronze@sadeazureproject.dfs.core.windows.net/FactStream")
df_stream = df_stream.withColumn("stream_name", upper(col("stream_name"))) 

df_stream_obj = Reusable()
df_stream = df_stream_obj.dropColumns(df_stream, ["_rescued_data"]);

df_stream = df_stream.dropDuplicates(["stream_id"]);

display(df_stream, checkpointLocation="abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/checkpoint")

df_stream.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/data")

df_stream.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@sadeazureproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_catalog.silver.FactStream")

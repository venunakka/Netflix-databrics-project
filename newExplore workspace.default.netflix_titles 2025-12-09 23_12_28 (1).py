# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 : Load the table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Read the table which I already registered in Unity Catalog

# COMMAND ----------

# use this (fully qualified table name)
df = spark.table("workspace.default.netflix_titles")   # catalog.schema.table
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Cleaing the code

# COMMAND ----------

from pyspark.sql import functions as F

df2 = df.dropDuplicates(["show_id"])    # remove duplicate rows
df2 = df2.withColumn("release_year", F.col("release_year").cast("int"))
df2 = df2.withColumn("date_added_clean", F.to_date("date_added", "MMMM d, yyyy"))
df2 = df2.withColumn("genres", F.split("listed_in", ", "))


# COMMAND ----------

# MAGIC %md
# MAGIC Create the database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS workspace.netflix_db;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS netflix_db;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Removes duplicates, casts release_year, cleans date_added (handles leading spaces and parsing errors), splits genres, adds ingestion timestamp, and writes to a Delta table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ----------------------------
# 1. Remove duplicates
# ----------------------------
df2 = df.dropDuplicates(["show_id"])

# ----------------------------
# 2. Cast release_year to integer
# ----------------------------
df2 = df2.withColumn("release_year", F.col("release_year").cast(IntegerType()))

# ----------------------------
# 3. Clean and parse date_added safely
#    - remove leading/trailing spaces
#    - use try_to_date to avoid parse errors
# ----------------------------
df2 = df2.withColumn(
    "date_added_clean",
    F.expr("try_to_date(regexp_replace(date_added, '^\\s+', ''), 'MMMM d, yyyy')")
)

# ----------------------------
# 4. Split genres into array
# ----------------------------
df2 = df2.withColumn("genres", F.split("listed_in", ", "))

# ----------------------------
# 5. Add ingestion timestamp
# ----------------------------
df2 = df2.withColumn("ingested_at", F.current_timestamp())

# ----------------------------
# 6. Write to Delta table (overwrite)
#    - make sure the database exists
# ----------------------------
catalog_db = "netflix_db"
table_name = "netflix_titles"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")

df2.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_db}.{table_name}")

print(f"✅ Successfully wrote Delta table: {catalog_db}.{table_name}")


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS netflix_db.netflix_titles")


# COMMAND ----------

# MAGIC %md
# MAGIC Clean and Transform

# COMMAND ----------

# MAGIC %md
# MAGIC Split country into an array (country_list)
# MAGIC
# MAGIC Lowercase categorical columns (type, rating, country)
# MAGIC
# MAGIC Keep the safe date parsing and genre splitting
# MAGIC
# MAGIC Add ingestion timestamp
# MAGIC
# MAGIC Write to Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ----------------------------
# 1. Remove duplicates
# ----------------------------
df2 = df.dropDuplicates(["show_id"])

# ----------------------------
# 2. Cast release_year to integer
# ----------------------------
df2 = df2.withColumn("release_year", F.col("release_year").cast(IntegerType()))

# ----------------------------
# 3. Clean and parse date_added safely
#    - remove leading spaces
#    - use try_to_date to avoid parse errors
# ----------------------------
df2 = df2.withColumn(
    "date_added_clean",
    F.expr("try_to_date(regexp_replace(date_added, '^\\s+', ''), 'MMMM d, yyyy')")
)

# ----------------------------
# 4. Split genres and countries into arrays
# ----------------------------
df2 = df2.withColumn("genres", F.split("listed_in", ", "))
df2 = df2.withColumn("country_list", F.split("country", ",\\s*"))

# ----------------------------
# 5. Lowercase categorical columns
#    - for strings: type, rating
#    - for arrays: country_list
# ----------------------------
for c in ["type", "rating"]:
    if c in df2.columns:
        df2 = df2.withColumn(c, F.lower(F.col(c)))

# lowercase each element in country_list array
if "country_list" in df2.columns:
    df2 = df2.withColumn(
        "country_list",
        F.expr("transform(country_list, x -> lower(x))")
    )

# ----------------------------
# 6. Add ingestion timestamp
# ----------------------------
df2 = df2.withColumn("ingested_at", F.current_timestamp())

# ----------------------------
# 7. Write to Delta table (overwrite)
#    - create database if not exists
# ----------------------------
catalog_db = "netflix_db"
table_name = "netflix_titles"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")

df2.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_db}.{table_name}")

print(f"✅ Delta table ready: {catalog_db}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC Quick Sanity Check

# COMMAND ----------

# Check schema
spark.table("netflix_db.netflix_titles").printSchema()

# Preview first 10 rows
spark.table("netflix_db.netflix_titles").show(10, truncate=False)

# Count total rows
spark.table("netflix_db.netflix_titles").count()


# COMMAND ----------

# MAGIC %md
# MAGIC Explore / Analyze the data

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution of genres

# COMMAND ----------

df = spark.table("netflix_db.netflix_titles")
df.select(F.explode("genres").alias("genre")) \
  .groupBy("genre").count() \
  .orderBy(F.desc("count")) \
  .show(20, False)


# COMMAND ----------

# MAGIC %md
# MAGIC Top Countries

# COMMAND ----------

df.select(F.explode("country_list").alias("country")) \
  .groupBy("country").count() \
  .orderBy(F.desc("count")) \
  .show(20, False)


# COMMAND ----------

# MAGIC %md
# MAGIC Titles by release year

# COMMAND ----------

df.groupBy("release_year").count() \
  .orderBy("release_year") \
  .show(20)


# COMMAND ----------

# MAGIC %md
# MAGIC Cleans your Netflix dataset, handles dates, arrays, removes duplicates, and writes to a Delta table safely without schema errors. This version uses try_to_date for date parsing, handles country_list as an array, and enables schema merge when writing.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

# 1️⃣ Read / assume df is your raw DataFrame
# df = spark.read.format("csv").option("header", True).load("/path/to/netflix.csv")

# 2️⃣ Remove duplicates
df2 = df.dropDuplicates(["show_id"])

# 3️⃣ Cast release_year to integer
df2 = df2.withColumn("release_year", F.col("release_year").cast(IntegerType()))

# 4️⃣ Clean date_added using try_to_date to avoid parsing errors
df2 = df2.withColumn("date_added_clean", F.expr("try_to_date(date_added, 'MMMM d, yyyy')"))

# 5️⃣ Split listed_in into genres array
df2 = df2.withColumn("genres", F.split("listed_in", ", "))

# 6️⃣ Split country into country_list array (handle NULLs)
df2 = df2.withColumn("country_list", F.when(F.col("country").isNotNull(), F.split(F.col("country"), ", ")).otherwise(F.array()))

# 7️⃣ Add ingestion timestamp
df2 = df2.withColumn("ingested_at", F.current_timestamp())

# 8️⃣ Write to Delta table with merge schema
df2.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("netflix_db.netflix_titles")

# 9️⃣ Quick check
spark.table("netflix_db.netflix_titles").show(5, truncate=False)
spark.table("netflix_db.netflix_titles").printSchema()

# 1️⃣0️⃣ Basic Analysis / Aggregations

# Total shows by type
df2.groupBy("type").count().orderBy(F.desc("count")).show()

# Top 10 genres
df2.select(F.explode("genres").alias("genre")) \
   .groupBy("genre").count() \
   .orderBy(F.desc("count")).show(10)

# Shows added per year
df2.groupBy("release_year").count().orderBy("release_year").show(20)

# Top 10 countries by number of shows
df2.select(F.explode("country_list").alias("country")) \
   .groupBy("country").count() \
   .orderBy(F.desc("count")).show(10)

# Rating distribution
df2.groupBy("rating").count().orderBy(F.desc("count")).show()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# 1️⃣ Read / assume df is your raw DataFrame
# df = spark.read.format("csv").option("header", True).load("/path/to/netflix.csv")

# 2️⃣ Remove duplicates
df2 = df.dropDuplicates(["show_id"])

# 3️⃣ Cast release_year to integer
df2 = df2.withColumn("release_year", F.col("release_year").cast(IntegerType()))

# 4️⃣ Clean date_added using try_to_date to avoid parsing errors
df2 = df2.withColumn("date_added_clean", F.expr("try_to_date(date_added, 'MMMM d, yyyy')"))

# 5️⃣ Split listed_in into genres array
df2 = df2.withColumn("genres", F.split("listed_in", ", "))

# 6️⃣ Split country into country_list array (handle NULLs)
df2 = df2.withColumn("country_list", F.when(F.col("country").isNotNull(), F.split(F.col("country"), ", ")).otherwise(F.array()))

# 7️⃣ Add ingestion timestamp
df2 = df2.withColumn("ingested_at", F.current_timestamp())

# 8️⃣ Write to Delta table with merge schema
df2.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("netflix_db.netflix_titles")

# 9️⃣ Quick check
display(spark.table("netflix_db.netflix_titles").limit(5))
spark.table("netflix_db.netflix_titles").printSchema()

# 🔹 Basic Analysis / Aggregations

# Total shows by type
df_type = df2.groupBy("type").count().orderBy(F.desc("count"))
display(df_type)

# Top 10 genres
df_genres = df2.select(F.explode("genres").alias("genre")) \
   .groupBy("genre").count() \
   .orderBy(F.desc("count"))
display(df_genres.limit(10))

# Shows added per year
df_year = df2.groupBy("release_year").count().orderBy("release_year")
display(df_year)

# Top 10 countries by number of shows
df_countries = df2.select(F.explode("country_list").alias("country")) \
   .groupBy("country").count() \
   .orderBy(F.desc("count"))
display(df_countries.limit(10))

# Rating distribution
df_rating = df2.groupBy("rating").count().orderBy(F.desc("count"))
display(df_rating)



# COMMAND ----------

# MAGIC %md
# MAGIC 1️. Top 10 countries by number of shows

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW netflix_top_countries AS
# MAGIC SELECT country_name, COUNT(*) AS total_shows
# MAGIC FROM netflix_db.netflix_titles
# MAGIC LATERAL VIEW explode(country_list) AS country_name
# MAGIC GROUP BY country_name
# MAGIC ORDER BY total_shows DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Show the results
# MAGIC SELECT * FROM netflix_top_countries;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2️. Count of shows by type (Movie / TV Show)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW netflix_type_count AS
# MAGIC SELECT type, COUNT(*) AS total_shows
# MAGIC FROM netflix_db.netflix_titles
# MAGIC GROUP BY type
# MAGIC ORDER BY total_shows DESC;
# MAGIC
# MAGIC -- Show the results
# MAGIC SELECT * FROM netflix_type_count;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3️. Top 10 genres

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW netflix_top_genres AS
# MAGIC SELECT genre_name, COUNT(*) AS total_shows
# MAGIC FROM netflix_db.netflix_titles
# MAGIC LATERAL VIEW explode(genres) AS genre_name
# MAGIC GROUP BY genre_name
# MAGIC ORDER BY total_shows DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Show the results
# MAGIC SELECT * FROM netflix_top_genres;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4️. Top 10 directors by number of shows

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW netflix_top_directors AS
# MAGIC SELECT director, COUNT(*) AS total_shows
# MAGIC FROM netflix_db.netflix_titles
# MAGIC WHERE director IS NOT NULL
# MAGIC GROUP BY director
# MAGIC ORDER BY total_shows DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Show the results
# MAGIC SELECT * FROM netflix_top_directors;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Count of shows of rating

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW netflix_rating_count AS
# MAGIC SELECT rating, COUNT(*) AS total_shows
# MAGIC FROM netflix_db.netflix_titles
# MAGIC GROUP BY rating
# MAGIC ORDER BY total_shows DESC;
# MAGIC
# MAGIC -- Show the results
# MAGIC SELECT * FROM netflix_rating_count;
# MAGIC
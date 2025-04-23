from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import to_date, col, avg

import sys

input_meteo = sys.argv[1]
input_pollution = sys.argv[2]
output_path = sys.argv[3]

spark = SparkSession.builder \
    .appName("FusionDonneesEnv") \
    .getOrCreate()

# Define schema for input_meteo
schema_meteo = StructType([
    StructField("Longitude (x)", StringType(), True),
    StructField("Latitude (y)", StringType(), True),
    StructField("Station Name", StringType(), True),
    StructField("Climate ID", StringType(), True),
    StructField("Date/Time (LST)", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Time (LST)", StringType(), True),
    StructField("Flag", StringType(), True),
    StructField("Temp (°C)", DoubleType(), True),
    StructField("Temp Flag", StringType(), True),
    StructField("Dew Point Temp (°C)", DoubleType(), True),
    StructField("Dew Point Temp Flag", StringType(), True),
    StructField("Rel Hum (%)", DoubleType(), True),
    StructField("Rel Hum Flag", StringType(), True),
    StructField("Precip. Amount (mm)", DoubleType(), True),
    StructField("Precip. Amount Flag", StringType(), True),
    StructField("Wind Dir (10s deg)", StringType(), True),
    StructField("Wind Dir Flag", StringType(), True),
    StructField("Wind Spd (km/h)", StringType(), True),
    StructField("Wind Spd Flag", StringType(), True),
    StructField("Visibility (km)", StringType(), True),
    StructField("Visibility Flag", StringType(), True),
    StructField("Stn Press (kPa)", StringType(), True),
    StructField("Stn Press Flag", StringType(), True),
    StructField("Hmdx", StringType(), True),
    StructField("Hmdx Flag", StringType(), True),
    StructField("Wind Chill", StringType(), True),
    StructField("Wind Chill Flag", StringType(), True),
    StructField("Weather", StringType(), True)
])

# Define schema for input_pollution
schema_pollution = StructType([
    StructField("Date", StringType(), True),
    StructField("Hour (UTC)", StringType(), True),
    StructField("FAFFD", DoubleType(), True)
])

# Load datasets with explicit schemas
df_meteo = spark.read.csv(input_meteo, header=True, schema=schema_meteo)
df_pollution = spark.read.csv(input_pollution, header=True, schema=schema_pollution)

# Keep only the first three columns in df_pollution
df_pollution = df_pollution.select("Date", "Hour (UTC)", "FAFFD")

# Clean columns names for each df
df_meteo = df_meteo.toDF(*[c.replace(".", "").replace("(", "").replace(")", "").replace(" ", "_") for c in df_meteo.columns])
df_pollution = df_pollution.toDF(*[c.replace(".", "").replace("(", "").replace(")", "").replace(" ", "_") for c in df_pollution.columns])

# Partition the data
df_meteo = df_meteo.repartition(4)
df_pollution = df_pollution.repartition(4)

# Cleaning null values
important_cols = ["Date/Time_LST", "Temp_°C", "Dew_Point_Temp_°C", "Rel_Hum_%", "Precip_Amount_mm"]
df_meteo = df_meteo.dropna(subset=important_cols)
df_pollution = df_pollution.dropna()

# Reformat the date column in df_meteo
df_meteo = df_meteo.withColumn("date", to_date(col("Date/Time_LST"), "yyyy-MM-dd HH:mm"))

# Renaming date column before joining
df_meteo = df_meteo.withColumnRenamed("date", "meteo_date")
df_pollution = df_pollution.withColumnRenamed("Date", "pollution_date")

print("Meteo dates sample:")
df_meteo.select("meteo_date").distinct().show(5)

print("Pollution dates sample:")
df_pollution.select("pollution_date").distinct().show(5)

# Join on both date
df_fusion = df_meteo.join(df_pollution, (df_meteo["meteo_date"] == df_pollution["pollution_date"]), how="inner")

# Aggregating values on columns
df_fusion_aggregated = df_fusion.groupBy("meteo_date").agg(
    avg("Temp_°C").alias("avg_Temp_°C"),
    avg("Dew_Point_Temp_°C").alias("avg_Dew_Point_Temp_°C"),
    avg("Rel_Hum_%").alias("avg_Rel_Hum_%"),
    avg("Precip_Amount_mm").alias("avg_Precip_Amount_mm"),
    avg("FAFFD").alias("avg_FAFFD")
)

# Selecting the relevant columns
df_fusion_aggregated = df_fusion_aggregated.select(
    col("meteo_date").alias("date"), "avg_Temp_°C", "avg_Dew_Point_Temp_°C", "avg_Precip_Amount_mm", "avg_FAFFD"
).orderBy("meteo_date")

# Partitionning before writing
df_fusion_aggregated = df_fusion_aggregated.repartition(4)

# Saving the result
df_fusion_aggregated.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

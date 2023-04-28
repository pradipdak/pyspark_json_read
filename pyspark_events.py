from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType, DoubleType, StructField

# create a SparkSession
spark = SparkSession.builder.appName("Big Data Processing JSON Files").getOrCreate()

# specify the path to the folder containing JSON files
folder_path = "C:/Users/dakpr/Downloads/events/events"

# define schema
new_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("type", StringType(), True),
    StructField("subtype", StringType(), True),
    StructField("eventId", StringType(), True),
    StructField("eventData", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("sensor", StringType(), True),
        StructField("sensorData", StructType([
            StructField("mileage", DoubleType(), True),
            StructField("fuelLevel", DoubleType(), True),
            StructField("fuelAmount", StringType(), True),
            StructField("batteryLevel", StringType(), True),
            StructField("remainingLife", StringType(), True),
            StructField("wheels", ArrayType(StructType([
                StructField("axle", IntegerType(), True),
                StructField("side", StringType(), True),
                StructField("position", StringType(), True),
                StructField("pressure", DoubleType(), True),
                StructField("uom", StringType(), True)
            ])))
        ]), True)
    ]), True),
    StructField("vehicleId", StringType(), True)
])

# read all JSON files from the folder
df = spark.read.format("json").option("multiLine", "true").schema(new_schema).load(folder_path)

#Add filename column to dataframe
df = df.withColumn("filename", input_file_name())

# print schema
print(df.printSchema())

# print number of records in dataframes
df.show(truncate=False)

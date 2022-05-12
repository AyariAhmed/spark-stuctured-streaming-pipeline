from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr


taxiFaresSchema = StructType([ \
  StructField("rideId", LongType()), StructField("taxiId", LongType()), \
  StructField("driverId", LongType()), StructField("startTime", TimestampType()), \
  StructField("paymentType", StringType()), StructField("tip", FloatType()), \
  StructField("tolls", FloatType()), StructField("totalFare", FloatType())])

taxiRidesSchema = StructType([ \
  StructField("rideId", LongType()), StructField("isStart", StringType()), \
  StructField("endTime", TimestampType()), StructField("startTime", TimestampType()), \
  StructField("startLon", FloatType()), StructField("startLat", FloatType()), \
  StructField("endLon", FloatType()), StructField("endLat", FloatType()), \
  StructField("passengerCnt", ShortType()), StructField("taxiId", LongType()), \
  StructField("driverId", LongType())])

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
      sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])


spark = SparkSession.builder \
  .appName("Spark Structured Streaming from Kafka") \
  .getOrCreate()

sdfRides = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "taxirides") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)") 

sdfFares = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "taxifares") \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value AS STRING)")
  
sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)
sdfFares = parse_data_from_kafka_message(sdfFares, taxiFaresSchema)


def rides_data_cleaning(ridesSdf):
  # remove all ride events that either started or ended outside NYC  
  LON_EAST, LON_WEST, LAT_NORTH, LAT_SOUTH = -73.7, -74.05, 41.0, 40.5
  ridesSdf = ridesSdf.filter( \
  ridesSdf["startLon"].between(LON_WEST, LON_EAST) & \
  ridesSdf["startLat"].between(LAT_SOUTH, LAT_NORTH) & \
  ridesSdf["endLon"].between(LON_WEST, LON_EAST) & \
  ridesSdf["endLat"].between(LAT_SOUTH, LAT_NORTH))
  
  # keep only finished ride events
  ridesSdf = ridesSdf.filter(ridesSdf["isStart"] == "END")
  return ridesSdf


sdfRides = rides_data_cleaning(sdfRides)

# watermarks are used for efficient joins
# Apply watermarks on event-time columns
sdfFaresWithWatermark = sdfFares \
  .selectExpr("rideId AS rideId_fares", "startTime", "totalFare", "tip") \
  .withWatermark("startTime", "30 minutes")  # maximal delay

sdfRidesWithWatermark = sdfRides \
  .selectExpr("rideId", "endTime", "driverId", "taxiId", \
    "startLon", "startLat", "endLon", "endLat") \
  .withWatermark("endTime", "30 minutes") # maximal delay


# Join with event-time constraints and aggregate
sdf = sdfFaresWithWatermark \
    .join(sdfRidesWithWatermark, \
      expr(""" 
       rideId_fares = rideId AND 
        endTime > startTime AND
        endTime <= startTime + interval 2 hours
        """))


query = sdf.groupBy("driverId").count()

query.writeStream \
  .outputMode("append") \
  .format("console") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()
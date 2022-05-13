'''
spark/bin/spark-submit   --master local --driver-memory 4g   --num-executors 2 --executor-memory 4g   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  sstreaming-spark-out.py 
'''


from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import udf
from pyspark.sql.functions import avg
from pyspark.sql.functions import window


taxiFaresSchema = StructType([
    StructField("rideId", LongType()), StructField("taxiId", LongType()),
    StructField("driverId", LongType()), StructField(
        "startTime", TimestampType()),
    StructField("paymentType", StringType()), StructField("tip", FloatType()),
    StructField("tolls", FloatType()), StructField("totalFare", FloatType())])

taxiRidesSchema = StructType([
    StructField("rideId", LongType()), StructField("isStart", StringType()),
    StructField("endTime", TimestampType()), StructField(
        "startTime", TimestampType()),
    StructField("startLon", FloatType()), StructField("startLat", FloatType()),
    StructField("endLon", FloatType()), StructField("endLat", FloatType()),
    StructField("passengerCnt", ShortType()), StructField(
        "taxiId", LongType()),
    StructField("driverId", LongType())])


def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    # split attributes to nested array in one Column
    col = split(sdf['value'], ',')
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])


spark = SparkSession.builder \
    .appName("Spark Structured Streaming from Kafka") \
    .getOrCreate()

# Subscription to kafka topics
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
    ridesSdf = ridesSdf.filter(
        ridesSdf["startLon"].between(LON_WEST, LON_EAST) &
        ridesSdf["startLat"].between(LAT_SOUTH, LAT_NORTH) &
        ridesSdf["endLon"].between(LON_WEST, LON_EAST) &
        ridesSdf["endLat"].between(LAT_SOUTH, LAT_NORTH))

    # keep only finished ride events
    ridesSdf = ridesSdf.filter(ridesSdf["isStart"] == "END")
    return ridesSdf


sdfRides = rides_data_cleaning(sdfRides)

# Watermark defines how much a timestamp can lag behind the maximum event time seen so far
# Are used for efficient joins
# Apply watermarks on event-time columns
sdfFaresWithWatermark = sdfFares \
    .selectExpr("rideId AS rideId_fares", "startTime", "totalFare", "tip") \
    .withWatermark("startTime", "30 minutes")  # maximal delay
# A Fares event would be kept up to 30 minutes to match it with Ride even
sdfRidesWithWatermark = sdfRides \
    .selectExpr("rideId", "endTime", "driverId", "taxiId",
                "startLon", "startLat", "endLon", "endLat") \
    .withWatermark("endTime", "30 minutes")  # maximal delay


# Join with event-time constraints and aggregate
sdf = sdfFaresWithWatermark \
    .join(sdfRidesWithWatermark,
          expr("""
       rideId_fares = rideId AND
        endTime > startTime AND
        endTime <= startTime + interval 2 hours
        """))

nbhds_df = spark.read.json("nbhd.jsonl")  # easy loading data
lookupdict = nbhds_df.select(
    "name", "coord").rdd.collectAsMap()  # cast the DataFrame
broadcastVar = spark.sparkContext.broadcast(
    lookupdict)  # use broadcastVar.value from now on

# Approx manhattan bbox
manhattan_bbox = [[-74.0489866963, 40.681530375], [-73.8265135518, 40.681530375],
                  [-73.8265135518, 40.9548628598], [-74.0489866963, 40.9548628598], [-74.0489866963, 40.681530375]]


def isPointInPath(x, y, poly):
    """check if point x, y is in poly
    poly -- a list of tuples [(x, y), (x, y), ...]"""
    num = len(poly)
    i = 0
    j = num - 1
    c = False
    for i in range(num):
        if ((poly[i][1] > y) != (poly[j][1] > y)) and \
            (x < poly[i][0] + (poly[j][0] - poly[i][0]) * (y - poly[i][1]) /
             (poly[j][1] - poly[i][1])):
            c = not c
        j = i
    return c


def find_nbhd(lon, lat):
    '''takes geo point as lon, lat floats and returns name of neighborhood it belongs to
          needs broadcastVar available'''
    if not isPointInPath(lon, lat, manhattan_bbox):
        return "Other"
    for name, coord in broadcastVar.value.items():
        if isPointInPath(lon, lat, coord):
            return str(name)  # cast unicode->str
    return "Other"  # geo-point not in neighborhoods


find_nbhd_udf = udf(find_nbhd, StringType())
sdf = sdf.withColumn("stopNbhd", find_nbhd_udf("endLon", "endLat"))
sdf = sdf.withColumn("startNbhd", find_nbhd_udf("startLon", "startLat"))

# query = sdf.groupBy("driverId").count()

# query.writeStream \
#   .outputMode("append") \
#   .format("console") \
#   .option("truncate", False) \
#   .start() \
#   .awaitTermination()

tips = sdf \
    .groupBy(
        window("endTime", "30 minutes", "10 minutes"),
        "stopNbhd") \
    .agg(avg("tip"))


tips.writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("tipss") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

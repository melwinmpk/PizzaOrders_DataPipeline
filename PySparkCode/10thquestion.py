from pyspark.sql import SparkSession
from pyspark.sql.functions import when, desc, asc,  to_date, to_timestamp, col, hour, dayofweek, weekofyear

#  8) How many pizzas were delivered that had both exclusions and extras?
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master("local[3]") \
        .getOrCreate()
    runner_orders_temp = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'False') \
        .option('inferSchema', 'True') \
        .load('hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/runner_orders_temp')
    runner_orders_temp = runner_orders_temp.withColumnRenamed("_c0", "order_id") \
        .withColumnRenamed("_c1", "runner_id") \
        .withColumnRenamed("_c2", "pickup_time") \
        .withColumnRenamed("_c3", "distance") \
        .withColumnRenamed("_c4", "duration") \
        .withColumnRenamed("_c5", "cancellation")


    customer_orders_temp = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'False') \
        .option('inferSchema', 'True') \
        .load('hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/customer_orders_temp')
    customer_orders_temp = customer_orders_temp.withColumnRenamed("_c0", "order_id") \
        .withColumnRenamed("_c1", "customer_id") \
        .withColumnRenamed("_c2", "pizza_id") \
        .withColumnRenamed("_c3", "exclusions") \
        .withColumnRenamed("_c4", "extras") \
        .withColumnRenamed("_c5", "order_time")


    successfull_orders = runner_orders_temp[['order_id']].where(runner_orders_temp["duration"] != 'NULL')


    customer_orders = customer_orders_temp.join(successfull_orders, \
                                       customer_orders_temp["order_id"] == successfull_orders["order_id"], \
                                       "inner")  \
                     .withColumn('up_order_time',to_timestamp(customer_orders_temp["order_time"], "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn('dayoftheweek',dayofweek(col('up_order_time')))\
                     .withColumn('weekofyear',weekofyear(col('up_order_time')))

    result = customer_orders.groupby('weekofyear', 'dayoftheweek').count()
    result_df = result.sort(asc('weekofyear'), asc('dayoftheweek'))
    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/order_dayofweek")

    # +----------+------------+-----+
    # | weekofyear | dayoftheweek | count |
    # +----------+------------+-----+
    # | 1 | 2 | 3 |
    # | 1 | 6 | 2 |
    # | 1 | 7 | 1 |
    # | 2 | 2 | 2 |
    # | 53 | 6 | 2 |
    # | 53 | 7 | 2 |
    # +----------+------------+-----+
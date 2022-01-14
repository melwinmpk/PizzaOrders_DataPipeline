from pyspark.sql import SparkSession
from pyspark.sql.functions import when, desc, to_date, to_timestamp, col, hour

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
                        .withColumn('up_order_time', to_timestamp(customer_orders_temp["order_time"], "yyyy-MM-dd HH:mm:ss")) \
                        .withColumn('houre', hour(col('up_order_time')))

    result_df = customer_orders.groupby('order_time', 'houre').count()

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/order_hour_wise")



    # +-------------------+-----+-----+
    # | up_order_time | houre | count |
    # +-------------------+-----+-----+
    # | 2021 - 01 - 01
    # 18: 05:02 | 18 | 1 |
    # | 2021 - 01 - 11
    # 18: 34:49 | 18 | 2 |
    # | 2021 - 01 - 02
    # 23: 51:23 | 23 | 2 |
    # | 2021 - 01 - 0
    # 9
    # 23: 54:33 | 23 | 1 |
    # | 2021 - 01 - 01
    # 19: 00:52 | 19 | 1 |
    # | 2021 - 01 - 04
    # 13: 23:46 | 13 | 3 |
    # | 2021 - 01 - 0
    # 8
    # 21: 20:29 | 21 | 1 |
    # | 2021 - 01 - 0
    # 8
    # 21: 00:29 | 21 | 1 |
    # +-------------------+-----+-----+
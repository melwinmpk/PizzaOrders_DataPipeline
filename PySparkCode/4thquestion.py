from pyspark.sql import SparkSession

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

    result_df = customer_orders_temp.join(  successfull_orders,\
                                customer_orders_temp["order_id"] == successfull_orders["order_id"],\
                              "inner").groupby('pizza_id').count()

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/type_pizza_delivered")


    # +--------+-----+
    # | pizza_id | count |
    # +--------+-----+
    # | 1 | 9 |
    # | 2 | 3 |
    # +--------+-----+

    # 4th question SELECT pizza_id, count(*) FROM customer_orders where order_id IN (SELECT order_id FROM runner_orders WHERE duration != 'NULL') GROUP BY pizza_id;
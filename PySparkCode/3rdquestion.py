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

    result_df = (runner_orders_temp[['runner_id']].where(runner_orders_temp["duration"] != 'NULL'))\
             .groupby('runner_id').count()

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/successful_order_by_runner")


    # +---------+-----+
    # | runner_id | count |
    # +---------+-----+
    # | 1 | 4 |
    # | 3 | 1 |
    # | 2 | 3 |
    # +---------+-----+

    # 3rd question SELECT runner_id, COUNT(*) FROM runner_orders WHERE duration != 'NULL' GROUP BY runner_id;
    # 4th question SELECT pizza_id, count(*) FROM customer_orders where order_id IN (SELECT order_id FROM runner_orders WHERE duration != 'NULL') GROUP BY pizza_id;
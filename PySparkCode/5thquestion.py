from pyspark.sql import SparkSession
from pyspark.sql.functions import when

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

    pizza_name = spark.read.format('csv') \
        .option('delimiter', ',') \
        .option('header', 'False') \
        .option('inferSchema', 'True') \
        .load('hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/pizza_names')
    pizza_name = pizza_name.withColumnRenamed("_c0", "pizza_id") \
                            .withColumnRenamed("_c1", "pizza_names")


    segregation_df = customer_orders_temp.withColumn("Meat Lovers",
                                                     when(customer_orders_temp["pizza_id"] == 1, 1).otherwise(0)) \
        .withColumn("Vegetarian", when(customer_orders_temp["pizza_id"] == 2, 1).otherwise(0))

    result_df = (segregation_df[["customer_id", "Meat Lovers", "Vegetarian"]].groupby('customer_id').sum()) \
        [["customer_id", "sum(Meat Lovers)", "sum(Vegetarian)"]]

    result_df = result_df.withColumnRenamed("sum(Meat Lovers)","Meat_Lovers_count")\
                        .withColumnRenamed("sum(Vegetarian)","Vegetarian_count")

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/pizza_type_ordercount")


    # +-----------+----------------+---------------+
    # | customer_id | sum(Meat
    # Lovers) | sum(Vegetarian) |
    # +-----------+----------------+---------------+
    # | 101 | 2 | 1 |
    # | 103 | 3 | 1 |
    # | 102 | 2 | 1 |
    # | 105 | 0 | 1 |
    # | 104 | 3 | 0 |
    # +-----------+----------------+---------------+
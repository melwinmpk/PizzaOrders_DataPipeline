from pyspark.sql import SparkSession
from pyspark.sql.functions import when, desc
from pyspark.sql.types import StructType, StructField, IntegerType

#  What was the maximum number of pizzas delivered in a single order?
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

    result = customer_orders_temp.join(successfull_orders, \
                                       customer_orders_temp["order_id"] == successfull_orders["order_id"], \
                                       "inner").groupby(customer_orders_temp["order_id"]).count().sort(desc('count'))

    result_df = result.first()
    data = [(result_df["order_id"], result_df["count"])]
    dataSchema = StructType([
        StructField('order_id', IntegerType(), True),
        StructField('count', IntegerType(), True)
    ])
    result_df = spark.createDataFrame(data=data, schema=dataSchema)

    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/max_delivery_pizza_count")

    # +--------+-----+
    # | order_id | count |
    # +--------+-----+
    # | 4 | 3 |
    # +--------+-----+
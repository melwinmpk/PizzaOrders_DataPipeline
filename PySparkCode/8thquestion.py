from pyspark.sql import SparkSession
from pyspark.sql.functions import when, desc

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

    result = customer_orders_temp.join(successfull_orders, \
                                       customer_orders_temp["order_id"] == successfull_orders["order_id"], \
                                       "inner") \
        .withColumn('ischange', when((
            (
                    (customer_orders_temp["exclusions"] == "'null'") |
                    (customer_orders_temp["exclusions"].isNull())
            ) |
            (
                    (customer_orders_temp["extras"].isNull()) |
                    (customer_orders_temp["extras"] == "'null'") |
                    (customer_orders_temp["extras"] == "'NaN'")
            )
    ), 'nochange').otherwise("change")).drop(successfull_orders["order_id"])
    result_df = result.where(result['ischange'] == 'change')
    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/order_with_exclusions_extras")
    result_df.show()

# +--------+-----------+--------+----------+------+-------------------+--------+
# |order_id|customer_id|pizza_id|exclusions|extras|         order_time|ischange|
# +--------+-----------+--------+----------+------+-------------------+--------+
# |      10|        104|       1|     '2|6'| '1|4'|2021-01-11 18:34:49|  change|
# +--------+-----------+--------+----------+------+-------------------+--------+
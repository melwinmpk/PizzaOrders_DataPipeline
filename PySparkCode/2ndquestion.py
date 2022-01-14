from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Streaming Word Count") \
        .master("local[3]") \
        .getOrCreate()

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
    customer_orders_temp.show()
    result = customer_orders_temp[['customer_id']].distinct().count()

    data = [(result,)]
    dataSchema = StructType([
        StructField('unique_customer_count', IntegerType(), True)
    ])
    result_df = spark.createDataFrame(data=data, schema=dataSchema)
    result_df.write.option("header", True) \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/user/hive/warehouse/pizza_db.db/unique_customer_count")


    # 14
    # 3rd question SELECT runner_id, COUNT(*) FROM runner_orders WHERE duration != 'NULL' GROUP BY runner_id;
    # 4th question SELECT pizza_id, count(*) FROM customer_orders where order_id IN (SELECT order_id FROM runner_orders WHERE duration != 'NULL') GROUP BY pizza_id;
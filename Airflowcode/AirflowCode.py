from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'melwin',
    'depends_on_past': False,
    'email': ['melwin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }
    
    
dag1 = DAG(
    'PizzaData_Ots',
    default_args=default_args,
    description='PizzaData OTS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
)

t1 = BashOperator(
task_id='loading_data_to_sql',
bash_command="""
mysql --local-infile=1 -uroot -pWelcome@123 -e"
create database if not exists pizza_db; 
use pizza_db; 

DROP TABLE if exists runners;
DROP TABLE if exists customer_orders;
DROP TABLE if exists runner_orders;
DROP TABLE if exists pizza_names;
DROP TABLE if exists pizza_recipes;
DROP TABLE if exists pizza_toppings;

create table if not exists runners( 
runner_id Int, 
registration_date date 
); 
SET GLOBAL local_infile=1; 

LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/runners.csv' 
INTO TABLE runners 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

create table if not exists customer_orders( 
order_id int, 
customer_id int, 
pizza_id int, 
exclusions varchar(10), 
extras varchar(10), 
order_time datetime 
); 

LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/customer_orders.csv' 
INTO TABLE customer_orders 
FIELDS TERMINATED BY ','
optionally enclosed by '\\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES; 

create table if not exists runner_orders( 
order_id int, 
runner_id int, 
pickup_time datetime, 
distance varchar(10), 
duration varchar(20), 
cancellation varchar(100) 
); 

LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/runner_orders.csv' 
INTO TABLE runner_orders 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES; 

create table if not exists pizza_names( 
pizza_id int, 
pizza_names varchar(20) 
); 

LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/pizza_names.csv' 
INTO TABLE pizza_names 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES; 

create table if not exists pizza_recipes( 
pizza_id int, 
toppings varchar(50) 
); 

LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/pizza_recipes.csv'
INTO TABLE pizza_recipes
FIELDS TERMINATED BY ','
optionally enclosed by '\\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

create table if not exists pizza_toppings( 
topping_id int, 
topping_name varchar(50) 
); 
LOAD DATA LOCAL INFILE '/home/saif/LFS/cohart-8/assignments/Assignment7thJan/data/pizza_toppings.csv' 
INTO TABLE pizza_toppings 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES;

"

""",
dag=dag1
)

t2 = BashOperator(
task_id='create_hive_tables',
bash_command=""" 
hive -e "
create database if not exists pizza_db; 
use pizza_db; 

drop table if exists runners;
drop table if exists customer_orders_temp;
drop table if exists runner_orders_temp;
drop table if exists pizza_names;
drop table if exists pizza_recipes_temp;
drop table if exists pizza_toppings;

create table if not exists runners( 
runner_id Int, 
registration_date string 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/runners'; 

create table if not exists customer_orders_temp( 
order_id int, 
customer_id int, 
pizza_id int, 
exclusions varchar(10), 
extras varchar(10), 
order_time string 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/customer_orders_temp'; 

create table if not exists runner_orders_temp( 
order_id int, 
runner_id int, 
pickup_time string, 
distance varchar(10), 
duration varchar(20), 
cancellation varchar(100) 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/runner_orders_temp'; 

create table if not exists pizza_names( 
pizza_id int, 
pizza_names varchar(20) 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/pizza_names'; 

create table if not exists pizza_recipes_temp( 
pizza_id int, 
toppings varchar(50) 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/pizza_recipes_temp'; 

create table if not exists pizza_toppings( 
topping_id int, 
topping_name varchar(50) 
) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/pizza_db.db/pizza_toppings';
"
""",
dag=dag1
)

t3 = BashOperator(
task_id='loading_data_from_sql_to_hive_via_sqoop',
bash_command=""" 

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query 'SELECT runner_id, registration_date FROM runners where $CONDITIONS ' \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/runners \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/runners \
--map-column-java registration_date=String 

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query 'SELECT order_id, customer_id, pizza_id, REPLACE(exclusions, ",", "|") as exclusions, REPLACE(extras, ",", "|") as extras, order_time FROM customer_orders where $CONDITIONS ' \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/customer_orders_temp \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/customer_orders_temp \
--map-column-java order_time=String

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query 'SELECT order_id, runner_id, pickup_time,distance, duration, cancellation FROM runner_orders where $CONDITIONS ' \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/runner_orders_temp \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/runner_orders_temp \
--map-column-java pickup_time=String

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table pizza_names \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/pizza_names \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/pizza_names

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query 'SELECT pizza_id, REPLACE(toppings, ",", "|") as toppings FROM pizza_recipes  where $CONDITIONS ' \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/pizza_recipes_temp \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/pizza_recipes_temp

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table pizza_toppings \
--m 1 \
--target-dir /user/hive/warehouse/pizza_db.db/pizza_toppings \
--delete-target-dir --target-dir /user/hive/warehouse/pizza_db.db/pizza_toppings

""",
dag=dag1
)

t4 = BashOperator(
task_id='Create_the_hive_table_for_the_10question',
bash_command="""
hive -e "
use pizza_db;  
drop table if exists pizza_order_count;
drop table if exists unique_customer_count;
drop table if exists successful_order_by_runner;
drop table if exists type_pizza_delivered;
drop table if exists pizza_type_ordercount;
drop table if exists max_delivery_pizza_count;
drop table if exists order_with_change_nochange;
drop table if exists order_with_exclusions_extras;
drop table if exists order_hour_wise;
drop table if exists order_dayofweek;


create table if not exists pizza_order_count(
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/pizza_order_count'
tblproperties('skip.header.line.count'='1');


create table if not exists unique_customer_count(
unique_customer_count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/unique_customer_count'
tblproperties('skip.header.line.count'='1');

create table if not exists successful_order_by_runner(
runner_id int,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/successful_order_by_runner'
tblproperties('skip.header.line.count'='1');

create table if not exists type_pizza_delivered(
pizza_id int,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/type_pizza_delivered'
tblproperties('skip.header.line.count'='1');


create table if not exists pizza_type_ordercount(
customer_id int,
Meat_Lovers_count int,
Vegetarian_count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/pizza_type_ordercount'
tblproperties('skip.header.line.count'='1');

create table if not exists max_delivery_pizza_count(
order_id int,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/max_delivery_pizza_count'
tblproperties('skip.header.line.count'='1');


create table if not exists order_with_change_nochange(
customer_id int,
ischange string,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/order_with_change_nochange'
tblproperties('skip.header.line.count'='1');

create table if not exists order_with_exclusions_extras(
order_id int,
customer_id int,
pizza_id int,
exclusions string,
extras string,
order_time string,
ischange string
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/order_with_exclusions_extras'
tblproperties('skip.header.line.count'='1');


create table if not exists order_hour_wise(
order_time string,
houre int,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/order_hour_wise'
tblproperties('skip.header.line.count'='1');


create table if not exists order_dayofweek(
weekofyear int,
dayoftheweek int,
count int
)
row format delimited fields terminated by ','
location '/user/hive/warehouse/pizza_db.db/order_dayofweek'
tblproperties('skip.header.line.count'='1');
"
""",
dag=dag1
)

t5 = BashOperator(
task_id='Spark_submit_for_the_10_questions',
bash_command=""" 
spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/1stquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/2ndquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/3rdquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/4thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/5thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/6thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/7thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/8thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/9thquestion.py

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 1 \
--executor-cores 4 \
/home/saif/LFS/cohart_c8/Assignment7thJan/10thquestion.py
""",
dag=dag1)

start = DummyOperator(task_id='start', dag=dag1)
end = DummyOperator(task_id='end', dag=dag1)

start >> t1 >> t2 >> t3 >> t4 >> t5 >> end

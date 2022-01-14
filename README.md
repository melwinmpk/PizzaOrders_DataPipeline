# PizzaOrders_DataPipeline
<p>There is a Tony who is owning  a New Pizza shop. <br>He knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire <br>so he had one more genius idea to combine with it - he was going to Uberize it - and so Pizza Runner was launched!</p>
<p>Tony started by recruiting “runners” to deliver fresh pizza from Pizza Runner Headquarters (otherwise known as Tony’s house) and also maxed out his credit card to pay freelance developers to build a mobile app to accept orders from customers. </p>
<p>Now he wants to know how is his business going on he needs some answers to his questions from the data. 
but the data which is stored is not in an appropriate format.
He Approaches a Data Engineer to process and store the data for him and get the answers to his question </p>
<p><h3>The data are stored in the different CSV files </h3></p>
<ul>
<li>
    <b>customer_orders.csv</b><br>
    Columns=>order_id,customer_id,pizza_id,exclusions,extras,order_time
    
</li>
<li>
    <b>pizza_names.csv</b><br>
    Columns=> pizza_id,pizza_name
</li>
<li>
    <b>pizza_recipes.csv</b><br>
    Columns=>pizza_id,toppings
</li>
<li>
    <b>pizza_toppings.csv</b><br>
    Columns=>topping_id,topping_name 
</li>
<li>
    <b>runner_orders.csv</b><br>
    Columns=>order_id,runner_id,pickup_time,distance,duration,cancellation<br>
    
</li>
<li>
    <b>runners.csv</b><br>
    Columns=> runner_id,registration_date<br>
</li>
</ul>    
<p><h2> The Answers the Tony wanted for </h2></p>
<ol>
<li>How many pizzas were ordered?</li>
<li>How many unique customer orders were made?</li>
<li>How many successful orders were delivered by each runner?</li>
<li>How many of each type of pizza was delivered?</li>
<li>How many Vegetarian and Meatlovers were ordered by each customer?</li>
<li>What was the maximum number of pizzas delivered in a single order?</li>
<li>For each customer, how many delivered pizzas had at least 1 change and how many
had no changes?</li>
<li>How many pizzas were delivered that had both exclusions and extras?</li>
<li>What was the total volume of pizzas ordered for each hour of the day?</li>
<li>Wh/at was the volume of orders for each day of the week?</li>
</ol>
<p><h2>Requirements</h2></p>
<ol>
<li>Store the data In MY SQL table </li>
<li>Using Sqoop Store the Data in Hive</li>
<li>Using the PySpark the get the Results for the question</li>
<li>Store the Results in Seperate Table</li>
<li>Automate entire process in the Airflow</li>
</ol>
<img src="https://github.com/melwinmpk/PizzaOrders_DataPipeline/blob/main/img/DataPipelineFlow.png?raw=true"></img>


<h2>AirFlow Output</h2>
<img src="https://github.com/melwinmpk/PizzaOrders_DataPipeline/blob/main/img/Airflow1.PNG?raw=true"></img>
<img src="https://github.com/melwinmpk/PizzaOrders_DataPipeline/blob/main/img/Airflow2.PNG?raw=true"></img>

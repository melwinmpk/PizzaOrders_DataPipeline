# PizzaOrders_DataPipeline
<p>There is a Tony who is owning  a New Pizza shop. <br>He knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire <br>so he had one more genius idea to combine with it - he was going to Uberize it - and so Pizza Runner was launched!</p>
<p>Tony started by recruiting “runners” to deliver fresh pizza from Pizza Runner Headquarters (otherwise known as Tony’s house) and also maxed out his credit card to pay freelance developers to build a mobile app to accept orders from customers. </p>
<p>Now he wants to know how is his business going on he needs some answers to his questions from the data. 
but the data which is stored is not in an appropriate format.
He Approaches a Data Engineer to process and store the data for him and get the answers to his question </p>
<p>the data are stored in the different CSV files </p>
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

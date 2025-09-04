'''
This timer triggered Azure Function reads historical sales data from a database, uses Spark to load and prepare the data,
then applies an FPGrowth model to spot the products that are often sold together, it also uses arima model to generate predictions on daily sales data. 
Finally, it writes the results back to the database.
'''

import azure.functions as func
import logging
from pyspark.sql import SparkSession
import jaydebeapi
import time
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
import os
import pandas as pd
from pmdarima.arima import auto_arima

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 16 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 

def cross_selling_and_prediction_model_timer_triggered(myTimer: func.TimerRequest) -> None:

    logging.info('Python HTTP trigger function processed a request.')

    #getting environment parameters
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    jdbcHostname = os.getenv("JDBC_HOSTNAME")
    jdbcPort = os.getenv("JDBC_PORT")
    jdbcDatabase = os.getenv("JDBC_DATABASE")
    minimum_support_int = os.getenv("MINIMUM_SUPPORT")
    minimum_confidence = os.getenv("MINIMUM_CONFIDENCE")
    prediction_span_in_days = os.getenv("PREDICTION_SPAN")

    list_parameters = {
    'username': username,
    'password': password,
    'jdbcHostname': jdbcHostname,
    'jdbcPort': jdbcPort,
    'jdbcDatabase': jdbcDatabase,
    'minimum_support': minimum_support_int,
    'minimum_confidence': minimum_confidence,
    'prediction_span_in_days': prediction_span_in_days
    }
    empty_parameters = [key for key, value in list_parameters.items() if not value]

    if len(empty_parameters)>0:
        raise ValueError("The function executed successfully, But the following parameters are empty: \n" + 
        ', '.join(empty_parameters))
    
    minimum_support_int = int(minimum_support_int)
    minimum_confidence = float(minimum_confidence)
    prediction_span_in_days = int(prediction_span_in_days)

    jdbc_driver_path = "mssql-jdbc-9.4.1.jre8.jar"

    # URL JDBC
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


    spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.network.timeout", "600s") \
    .appName("MyApp") \
    .getOrCreate()

    connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    max_retry_attempts = 3
    n_iteration = 1

    sales_query = "(select date, amount, documentKey,sellToCustomerKey,productKey from dbo.invoice) as temp"
    customer_query = "(select customerKey,customerId from dbo.customer) as temp"
    product_query = "(select productKey,productFull from dbo.product) as temp"

    while n_iteration <= max_retry_attempts:
        # read tables
        try:
            sales_df = spark.read.jdbc(url=jdbcUrl, table=sales_query, properties=connectionProperties)
            customers_df = spark.read.jdbc(url=jdbcUrl, table=customer_query, properties=connectionProperties)
            products_df = spark.read.jdbc(url=jdbcUrl, table=product_query, properties=connectionProperties)
        except:
            print(f"Attempt {n_iteration}: Connection Falied, trying again in 5 seconds...")
            time.sleep(5)
            n_iteration +=1
            if n_iteration > max_retry_attempts:
                raise ValueError("Connection Failed with Database, check settings")
        else:
            print("Connection successful, tables loaded")
            break

    
    ######################### CROSS SELLING MODEL #####################################################

    #### JOIN WITH DIMENSION TABLES ####
    merged_df = sales_df.join(
        customers_df,
        sales_df["sellToCustomerKey"] == customers_df["customerKey"],
        how="left"
    )

    merged_df = merged_df.join(
        products_df,
        on="productKey",
        how="left"
    )


    # Group products per customer
    transactions_df = (merged_df
        .groupBy("documentKey")
        .agg(F.collect_set("productKey").alias("items")) # collect_set to get unique items
    )


    min_number_of_elements = minimum_support_int
    minimum_support = min_number_of_elements/transactions_df.count()

    # Apply FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=minimum_support, minConfidence=minimum_confidence)
    model = fpGrowth.fit(transactions_df)

    # association rules.
    df_association = model.associationRules

    #filter only pairwise association
    df_association = df_association.filter(
        (F.size("antecedent") == 1) & (F.size("consequent") == 1)
    )

    df_association = df_association.orderBy("confidence",ascending=False)

    #extract element from list
    df_association = df_association.withColumn(
        "antecedent", F.expr("element_at(antecedent, 1)")
    ).withColumn(
        "consequent", F.expr("element_at(consequent, 1)")
    )

    df_association = df_association.withColumnRenamed('antecedent','antecedentProductKey').withColumnRenamed('consequent','consequentProductKey')
    df_association = df_association.withColumn("insertTimestamp", F.current_timestamp())

    ############################## END CROSS SELLING ########################################################

    ############################## PREDICTION MODEL ##########################################################

    sales_df = sales_df.withColumn(
        "date",
        F.to_date("date", "yyyy-MM-dd HH:mm:ss")
    )

    # Filter last year of data
    max_date = sales_df.select(F.max("date")).collect()[0][0]
    last_year_start = (max_date - F.expr("INTERVAL 1 YEAR"))
    sales_df_by_day = sales_df.groupBy("date").agg(F.sum("amount").alias("total_amount")).filter((F.col("date") >= last_year_start))

    ############ fill empty values if missing #################
    date_limits = sales_df_by_day.select(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date")
    ).collect()[0]

    min_date, max_date = date_limits.min_date, date_limits.max_date

    # Create a DataFrame with all the dates
    date_df = (
        spark
        .createDataFrame([(min_date, max_date)], ["start", "end"])
        .select(F.explode(F.sequence("start", "end")).alias("date"))
    )

    # perform join with the original dataframe
    sales_df_by_day = (
        date_df
        .join(sales_df_by_day, on="date", how="left")
        .na.fill(0, subset=["total_amount"])
    )

    # Convert the PySpark DataFrame to Pandas
    sales_df_by_day_pd = sales_df_by_day.orderBy("date").limit(1000).toPandas()

    #set index in the dataframe
    sales_df_by_day_pd['date'] = pd.to_datetime(sales_df_by_day_pd['date'])

    # add future dates
    test_length = int(prediction_span_in_days)

    last_date = sales_df_by_day_pd['date'].iloc[-1]

    # Create the range of dates
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), 
                                periods=test_length, 
                                freq='D')

    # Create fictitious dataframe
    df_future = pd.DataFrame({
        'date': future_dates, 
        'total_amount': [0.0] * test_length
    })

    # concat the two dfs
    extended_df = pd.concat([sales_df_by_day_pd, df_future])

    sales_df_pd = extended_df.set_index("date").sort_index()

    # split train and test
    train_data = sales_df_pd[:-test_length]
    test_data = sales_df_pd[-test_length:]


    # creation arima model
    arima_model = auto_arima(train_data, start_p = 0, d=1 , start_q = 0,
                            max_p =7, max_d=2, max_q = 7, start_P = 0,
                            D=1, start_Q = 0, max_P =7, max_D = 2, max_Q = 7, max_order = None,
                            m = 7, seasonal = True)

    # arima_model.summary()

    ## forecasting on test data
    prediction = pd.DataFrame(arima_model.predict(n_periods = test_length), index = test_data.index , columns=['predicted_sales'])

    # Convert Pandas DataFrame to Spark DataFrame
    prediction.reset_index(inplace=True)
    spark_df_prediction = spark.createDataFrame(prediction)
    spark_df_prediction = spark_df_prediction.withColumn("insert_timestamp", F.current_timestamp())

    ############################# END PREDICTION MODEL #######################################################


    #### TRUNCATE TABLES BEFORE LOADING ####
    # Connection parameters
    driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # Connection to database through jdbc
    conn = jaydebeapi.connect(
        driver_class,
        jdbcUrl,
        [username, password],
        jdbc_driver_path
    )
    # Creation of the cursor
    cursor = conn.cursor()
    # TRUNCATE TABLE before loading new data
    cursor.execute("TRUNCATE TABLE [dbo].[crossSelling]")
    cursor.execute("TRUNCATE TABLE [dbo].[predictions]")
    # Close cursor and connection
    cursor.close()
    conn.close()

    #### LOADING DATA INTO SQL SERVER ####
    # append data into sql server
    df_association.write.jdbc(
        url=jdbcUrl,
        table="dbo.crossSelling",  
        mode="append",       
        properties=connectionProperties
    )

    spark_df_prediction.write.jdbc(
        url=jdbcUrl,
        table="dbo.predictions",  
        mode="append",       
        properties=connectionProperties
    )

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
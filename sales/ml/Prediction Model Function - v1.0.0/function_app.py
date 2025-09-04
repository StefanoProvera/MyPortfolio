'''
This Azure Function reads historical sales data from a database, uses Spark to load and prepare the data,
then applies an ARIMA model to predict future sales. Finally, it writes the prediction results back to the database.
'''

import azure.functions as func
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pmdarima.arima import auto_arima
import jaydebeapi
import time


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="demo_sales_model")
def demo_sales_model(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Username e password
    username = "***"
    password = "***"

    jdbcHostname = req.params.get('jdbcHostname')
    jdbcPort = req.params.get('jdbcPort')
    jdbcDatabase = req.params.get('jdbcDatabase')
    prediction_span_in_days = req.params.get('prediction_span_in_days')
    sales_table = req.params.get('sales_table')

    list_parameters = {
    'jdbcHostname': jdbcHostname,
    'jdbcPort': jdbcPort,
    'jdbcDatabase': jdbcDatabase,
    'prediction_span_in_days': prediction_span_in_days,
    'sales_table': sales_table
    }
    empty_parameters = [key for key, value in list_parameters.items() if not value]

    if len(empty_parameters)>0:
        return func.HttpResponse(
        "This HTTP triggered the function successfully, But the following parameters are empty: \n" + 
        ', '.join(empty_parameters),
        status_code=400
        )

    jdbc_driver_path = "mssql-jdbc-9.4.1.jre8.jar"

    spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.network.timeout", "600s") \
    .appName("MyApp") \
    .getOrCreate()

    # URL JDBC
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

    connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    max_retry_attempts = 3
    n_iteration = 1

    query = f"(SELECT date, amount FROM {sales_table}) as temp"

    while n_iteration <= max_retry_attempts:
        # read tables
        try:
            sales_df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
        except:
            print(f"Attempt {n_iteration}: Connenction Falied, trying again in 5 seconds...")
            time.sleep(5)
            n_iteration +=1
            if n_iteration > max_retry_attempts:
                return func.HttpResponse(
                "This HTTP triggered the function successfully, but function was unable to load data from Sql Server.",
                status_code=400
                )
        else:
            print("Connection successful, tables loaded")
            break
            
    #### CREATION ARIMA MODEL & FORECASTING ####
    # convert date to date format and filter out non relevant period of time

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
    sales_df_by_day_pd = sales_df_by_day.orderBy("date").toPandas()

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


    #### TRUNCATE TABLE BEFORE LOADING ####
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
    cursor.execute("TRUNCATE TABLE [dbo].[predictions]")
    # Close cursor and connection
    cursor.close()
    conn.close()

    #### LOADING DATA INTO SQL SERVER ####
    # append data into sql server
    spark_df_prediction.write.jdbc(
        url=jdbcUrl,
        table="dbo.predictions",  
        mode="append",       
        properties=connectionProperties
    )

    return func.HttpResponse(
        "This HTTP triggered function executed successfully.  \n" +
        str(spark_df_prediction.head()),
        status_code=200
    )
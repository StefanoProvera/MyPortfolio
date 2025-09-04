'''
This timer triggered Azure Function reads historical sales data from a database, uses Spark to load and prepare the data,
then applies an FPGrowth model to spot the products that are often sold together. Finally, it writes the results back to the database.
'''

import azure.functions as func
import logging
from pyspark.sql import SparkSession
import jaydebeapi
import time
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth



app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="crossSellingModel")
def demo_sales_model(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Username e password
    username = "***"
    password = "***"

    jdbcHostname = req.params.get('jdbcHostname')
    jdbcPort = req.params.get('jdbcPort')
    jdbcDatabase = req.params.get('jdbcDatabase')
    minimum_support_int = int(req.params.get('minimum_support'))
    minimum_confidence = float(req.params.get('minimum_confidence'))

    list_parameters = {
    'jdbcHostname': jdbcHostname,
    'jdbcPort': jdbcPort,
    'jdbcDatabase': jdbcDatabase,
    'minimum_support': minimum_support_int,
    'minimum_confidence': minimum_confidence
    }
    empty_parameters = [key for key, value in list_parameters.items() if not value]

    if len(empty_parameters)>0:
        return func.HttpResponse(
        "This HTTP triggered the function successfully, But the following parameters are empty: \n" + 
        ', '.join(empty_parameters),
        status_code=400
        )

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

    sales_query = "(select documentKey,sellToCustomerKey,productKey from dbo.invoice) as temp"
    customer_query = "(select customerKey,customerId from dbo.customer) as temp"
    product_query = "(select productKey,productFull from dbo.product) as temp"

    while n_iteration <= max_retry_attempts:
        # read tables
        try:
            sales_df = spark.read.jdbc(url=jdbcUrl, table=sales_query, properties=connectionProperties)
            customers_df = spark.read.jdbc(url=jdbcUrl, table=customer_query, properties=connectionProperties)
            products_df = spark.read.jdbc(url=jdbcUrl, table=product_query, properties=connectionProperties)
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
    cursor.execute("TRUNCATE TABLE [dbo].[crossSelling]")
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

    return func.HttpResponse(
        "This HTTP triggered function executed successfully.  \n" +
        str(df_association.head()),
        status_code=200
    )
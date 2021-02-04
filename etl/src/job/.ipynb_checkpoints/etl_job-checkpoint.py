"""
Author: Kamrul Hasan
Email: hasana.alive@gmail.com
Date: 20.12.2020
"""

"""
etl_job.py
~~~~~~~~~~
This Python module contains an  Apache Spark ETL job definition
that implements  ETL jobs. It can be
submitted to a Spark cluster in the spark docker container. 

etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

"""

# import the necessary dependencies

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from builtins import round
import json



def main():
    """Main ETL script definition.

    returns
    -------
            None
    """

    # read the configuraton file for the database configuration and input file 
    config_path = "/home/jovyan/work/src/configs/etl_config.json"
    #read and parse the config json file

    config_data= load_config(config_path)

    #set the app name and configuration for spark
    appName = "TakeyWay-ETL1"
    master = "local"

    conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master)

    # start Spark application and get Spark session

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession


    # execute ETL pipeline

    #Extract the data using json struct and pyspark dataframe.
    data = extract_data(spark,config_data)
    #flatten the json data into simpleer data frame
    sparkdf = flatten_data(data)
    # do the transformation with some aggregate functions
    data_transformed = transform_data(sparkdf)
    #load the data back to some storgae ex: postgres
    load_data(data_transformed,config_data)

    #terminate Spark application
    spark.stop()
    return None


def extract_data(spark,config_data):
    """Load data from json file format.
    
    Parameters
    ----------
         spark: Spark session object.

    Return
    ------
         df:Spark DataFrame.
    """
    path = config_data["input_data_path"]
    df = spark.read.json(path,multiLine = "false")

    return df

def flatten_data(df):
    """Flatten the data into more simpler dataframe to do the transformation.
    
    Parameters
    ----------
         df: Spark session object.

    Return
    ------
         final_df:Spark DataFrame.
    """
    temp_df = df.select("customerId",explode("orders.basket").alias("nbasket"),
                    col("orders.orderId").alias("ordersId"))

    final_df = (temp_df
        .select("customerId",explode("ordersId").alias("ordersId"),"nbasket")
        .select("customerId","ordersId",explode("nbasket").alias("new_topping"))
        .select("customerId","ordersId","new_topping.*")
        .withColumnRenamed("grossMerchandiseValueEur", "grossMerchandiseValueEur")
        .withColumnRenamed("productId", "productId")
        .withColumnRenamed("productType", "productType")
        )
    

    return final_df


def transform_data(ncdf):
    """Tranform the data by using some functions to do the aggregation and calculation to get the netspending of each user.
    
    Parameters
    ----------
         ncdf: Spark flatten dataframe.

    Return
    ------
         df_transformed: Tranformed Spark DataFrame.
    """
    fdf= ncdf.select("*",when(ncdf.productType == "hot food",
                          func.round(ncdf.grossMerchandiseValueEur+ncdf.grossMerchandiseValueEur*7/100,2))
                 .when(ncdf.productType == "cold food",
                       func.round(ncdf.grossMerchandiseValueEur+ncdf.grossMerchandiseValueEur*15/100,2))
                 .otherwise(func.round(ncdf.grossMerchandiseValueEur+ncdf.grossMerchandiseValueEur*9/100,2))
                 .alias('TotalMerchandiseValueEur'))
    df_transformed = fdf.groupBy("customerId").agg(func.round(sum("TotalMerchandiseValueEur"),2)
                                                  .alias("TotalMerchandiseValueEur"),
                                                  count("ordersId").alias("orders"))

    return df_transformed


def load_data(df,config_data):
    """Establised connection to the PostgreSQL and write the transformed data into specified table.
    
    Parameters
    ----------
         df: Spark transformed dataframe.

    Return
    ------
         None
    """

    # Databse configuration details.
    database = config_data['db_name']
    db_table =config_data['db_table']
    user = config_data['db_username']
    password  = config_data['db_password']
    port = config_data['db_port']
    mode = "overwrite"
    driver = "org.postgresql.Driver"
    url = f"jdbc:postgresql://{user}:{port}/{database}"

    # writing the transformed data directly into the postgres database table using the column names.

    df.select('customerId', 'TotalMerchandiseValueEur', 'orders').write.format("jdbc") \
      .mode("overwrite") \
      .option("url", url) \
      .option("dbtable", db_table) \
      .option("user", user) \
      .option("password", password) \
      .save()
    return None


def load_config(path):
    '''
    This function loads the database configuration file to establish database connection.

    Parameters
    ----------
    path : str
        This argument is a for reading the json configuration file

    Returns
    -------
    config: json
        The return is configuration details in a json format containing the details like database name, table, usrname,password

    '''
    
    with open(path, 'r') as j:
        config = json.loads(j.read())

    return config


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

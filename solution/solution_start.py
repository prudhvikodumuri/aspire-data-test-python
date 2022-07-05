import argparse
import time

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def create_spark_views(spark: SparkSession, customers_location: str, products_location: str,
                       transactions_location: str):
    spark.read.csv(customers_location, header=True).createOrReplaceTempView("customers")
    spark.read.csv(products_location, header=True).createOrReplaceTempView("products")
    spark.read.json(transactions_location).createOrReplaceTempView("raw_transactions")

def show_view(view_name: str, spark: SparkSession):
    view_df = spark.sql("""SELECT *  FROM """+view_name)
    view_df.show()

def view_to_df(view_name: str, spark: SparkSession):
    df = spark.sql("""SELECT *  FROM """+view_name)
    df

def run_transformations(spark: SparkSession, customers_location: str, products_location: str,
                        transactions_location: str, output_location: str):
    create_spark_views(spark, customers_location, products_location, transactions_location)


def get_latest_transaction_date(spark: SparkSession):
    result = spark.sql("""SELECT MAX(date_of_purchase) AS date_of_purchase FROM raw_transactions""").collect()[0]
    max_date = result.date_of_purchase
    return max_date

def process_dataframes(customers_df : DataFrame, raw_transactions_df: DataFrame, products_df: DataFrame):
    transactions_df = raw_transactions_df.withColumn('new_basket', explode("basket")).select('customer_id', 'date_of_purchase', 'new_basket.price', 'new_basket.product_id')
    transactions_join_df = \
        transactions_df.join(customers_df, transactions_df.customer_id == customers_df.customer_id, "inner")\
        .join(products_df, transactions_df.product_id == products_df.product_id, "inner")\
        .select(customers_df.customer_id, customers_df.loyalty_score, products_df.product_id, products_df.product_category)
    transactions_group_df = transactions_join_df.groupBy("customer_id", "loyalty_score","product_id", "product_category").count().withColumnRenamed('count','purchase_count')
    return transactions_group_df

def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')


if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/products.csv")
    #parser.add_argument('--transactions_location', required=False, default="C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/transactions/")
    parser.add_argument('--transactions_location', required=False, default="C:/Users/sathesh/Documents/Hunt/SainsburyDSE/starter/transactions_new/d=2022-04-30/transactions.json")



    parser.add_argument('--output_location', required=False, default="C:/Users/sathesh/Documents/Hunt/SainsburyDSE/starter/output_data/outputs/")
    args = vars(parser.parse_args())

    run_transformations(spark_session, args['customers_location'], args['products_location'],
                        args['transactions_location'], args['output_location'])

    show_view("raw_transactions", spark_session)
    customers_df = spark_session.read.csv(args['customers_location'], header=True)
    products_df = spark_session.read.csv(args['products_location'], header=True)
    transactions_df = spark_session.read.json(args['transactions_location'])

    process_dataframes(customers_df, transactions_df, products_df).write.csv(args['output_location'])



    time.sleep(600)

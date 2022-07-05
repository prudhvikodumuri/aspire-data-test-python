from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

from solution.solution_start import get_latest_transaction_date,process_dataframes




@pytest.mark.usefixtures("spark")
def test_group_transactions_data(spark):
    customers_df = spark.read.csv(
        "C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/customers.csv", header=True)
    products_df = spark.read.csv(
        "C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/products.csv", header=True)
    transactions_df = spark.read.json(
        "C:/Users/sathesh/Documents/Hunt/SainsburyDSE/starter/transactions_new/d=2022-04-30/transactions.json")
    actual_df = process_dataframes(customers_df, transactions_df, products_df)
    expected_df = spark.read.csv("C:/Users/sathesh/Documents/Hunt/SainsburyDSE/starter/expected_file.csv",
                                 header=True).withColumn('purchase_count', col('purchase_count').cast(IntegerType()))
    assert sorted(expected_df.collect()) == sorted(actual_df.collect())

def test_group_transactions_count(spark):
    customers_df = spark.read.csv(
        "C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/customers.csv", header=True)
    products_df = spark.read.csv(
        "C:/Projects/aspire-data-test-python/input_data_generator/input_data/starter/products.csv", header=True)
    transactions_df = spark.read.json(
        "C:/Users/sathesh/Documents/Hunt/SainsburyDSE/starter/transactions_new/d=2022-04-30/transactions.json")
    actual_df = process_dataframes(customers_df, transactions_df, products_df)
    assert 50 == actual_df.count()



def test_get_latest_transaction_date_returns_most_recent_date(spark):
    spark.createDataFrame([
        Row(date_of_purchase=datetime(2018, 12, 1, 4, 15, 0)),
        Row(date_of_purchase=datetime(2019, 3, 1, 14, 10, 0)),
        Row(date_of_purchase=datetime(2019, 2, 1, 14, 9, 59)),
        Row(date_of_purchase=datetime(2019, 1, 2, 19, 14, 20))
    ]).createOrReplaceTempView("raw_transactions")

    expected = datetime(2019, 3, 1, 14, 10, 0)
    actual = get_latest_transaction_date(spark)

    assert actual == expected

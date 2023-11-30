import json, os, re, sys, logging
from typing import (
    Callable,
    Optional,
    Any,
)  # This has been added from Python 3 onwards to add type hints
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode, date_format

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_path}/logs/jobs-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - LINE:%(lineno)d - %(process)d - %(thread)d - %(message)s"

sys.path.insert(0, project_path)
from classes.class_pyspark import SparkClass

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger("py4j")


def main(project_dir: str) -> None:
    """_summary_

    Parameters
    ----------
    project_dir : str
        _description_
    """
    config = open_config(f"{project_dir}/config/etl_config.json")
    spark = start_spark(config)
    transactions_df = import_data(
        spark, f"{project_dir}/test-data/sales/transactions", ".json$"
    )
    customer_df = import_data(spark, f"{project_dir}/test-data/sales/customers.csv")
    products_df = import_data(spark, f"{project_dir}/test-data/sales/products.csv")
    transform_data(spark, transactions_df, customer_df, products_df)
    stop_spark(spark)


def open_config(filepath: str) -> dict:
    """_summary_

    Parameters
    ----------
    filepath : str
        _description_

    Returns
    -------
    dict
        _description_
    """
    if isinstance(filepath, str) and os.path.exists(filepath):
        return SparkClass(conf={}).open_json(filepath)


def start_spark(config: dict) -> SparkSession:
    """_summary_

    Parameters
    ----------
    config : dict
        _description_

    Returns
    -------
    SparkSession
        _description_
    """
    if isinstance(config, dict):
        spark = SparkClass(conf={}).spark_start(config)
        return spark


def stop_spark(spark: SparkSession) -> None:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    """
    spark.stop()


def import_data(
    spark: SparkSession, datapath: str, pattern: Optional[str] = None
) -> DataFrame:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    datapath : str
        _description_
    pattern : Optional[str], optional
        _description_, by default None

    Returns
    -------
    DataFrame
        _description_
    """
    if isinstance(spark, SparkSession):
        return SparkClass(conf={}).import_data(spark, datapath, pattern)


def showMySchema(df: DataFrame, file_name: str) -> None:
    """_summary_

    Parameters
    ----------
    df : DataFrame
        _description_
    """
    if isinstance(df, DataFrame):
        SparkClass(conf={}).debug_df(df, file_name)


def transform_data(
    spark: SparkSession,
    transactions_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    df : DataFrame
        _description_

    Returns
    -------
    DataFrame
        _description_
    """

    # clean_transformations(transactions_df)
    # clean_customer(customers_df)
    # clean_products(products_df)
    create_temp_tables(
        spark,
        [
            ("transactions", clean_transactions(transactions_df)),
            ("customers", clean_customer(customers_df)),
            ("products", clean_products(products_df)),
        ],
    )

    export_result(
        spark,
        [
            (
                "transactions",
                clean_transactions(transactions_df),
            ),
            ("customers", clean_customer(customers_df)),
            ("products", clean_products(products_df)),
        ],
    )


def clean_transactions(df: DataFrame) -> DataFrame:
    """_summary_

    Parameters
    ----------
    df : DataFrame
        _description_

    Returns
    -------
    DataFrame
        _description_
    """
    if isinstance(df, DataFrame):
        exploded_df = df.withColumn("basket_explode", explode(col("basket"))).drop(
            "basket"
        )
        selected_df = exploded_df.select(
            col("customer_id"), col("basket_explode.*"), col("date_of_purchase")
        )
        transformed_df = (
            selected_df.withColumn("date", col("date_of_purchase").cast("Date"))
            .withColumn("price", col("price").cast("Integer"))
            .withColumn("time", date_format(col("date_of_purchase"), "HH:mm:ss"))
        )
        showMySchema(transformed_df, "transactions_df.json")
        return transformed_df


def clean_customer(df: DataFrame) -> DataFrame:
    """_summary_

    Parameters
    ----------
    df : DataFrame
        _description_

    Returns
    -------
    DataFrame
        _description_
    """
    if isinstance(df, DataFrame):
        clean_df = df.withColumn("loyalty_score", col("loyalty_score").cast("Integer"))
        showMySchema(clean_df, "customer_df.json")
        return clean_df


def clean_products(df: DataFrame) -> DataFrame:
    """_summary_

    Parameters
    ----------
    df : DataFrame
        _description_

    Returns
    -------
    DataFrame
        _description_
    """
    if isinstance(df, DataFrame):
        showMySchema(df, "products_df.json")
        return df


def create_temp_tables_kwargs(spark: SparkSession, **kwargs: Any) -> None:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    """
    kwargs["df"]["transactions"].createOrReplaceTempView("transactions")
    kwargs["df"]["customers"].createOrReplaceTempView("customers")
    kwargs["df"]["products"].createOrReplaceTempView("products")
    print(spark.catalog.listTables())


def create_temp_tables(spark: SparkSession, list_of_dfs: list) -> None:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    list_of_dfs : list
        _description_
    """
    new_list_of_dfs = [
        (lambda tuple_of_df: SparkClass(conf={}).create_temp_tables(tuple_of_df))(
            tuple_of_df
        )
        for tuple_of_df in list_of_dfs
    ]
    # debug_temp_tables = [
    #     (lambda tuple_of_tables: SparkClass(conf={}).debug_tables(tuple_of_tables))(
    #         tuple_of_tables
    #     )
    #     for tuple_of_tables in spark.catalog.listTables()
    # ]


def export_result(spark: SparkSession, list_of_dfs: list) -> None:
    """_summary_

    Parameters
    ----------
    spark : SparkSession
        _description_
    list_of_dfs : list
        _description_
    """
    t = [
        (
            lambda x: SparkClass(
                conf={"export": f"{project_path}/tmp/spark/delta/sales"}
            ).export_data(x)
        )(x)
        for x in list_of_dfs
    ]


if __name__ == "__main__":
    main(project_path)

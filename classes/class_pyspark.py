import json, os, re, sys
from typing import (
    Callable,
    Optional,
)  # This has been added from Python 3 onwards to add type hints
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkContext


class SparkClass:
    """This is a spark class that can be used to start a spark session and read data from various sources."""

    def __init__(self, conf) -> None:
        self.conf = conf

    def debug_df(self, df: DataFrame, file_name: str) -> None:
        print(file_name)

    def spark_start(
        self, kwargs: dict
    ) -> SparkSession:  # def spark_start(self, **kwargs) -> SparkSession:
        """
            In general sense **kwargs sends a dictionary of keyword arguments to a function. But we can be more specific if we know
            what kind of arguments we are going to send to the function. For example, if we know that we are going to send a dictionary
            then we can use kwargs:dict in place of **kwargs.
        Returns
        -------
        SparkSession
            _description_
        """
        MASTER = kwargs["spark_conf"]["master"]
        APP_NAME = kwargs["spark_conf"]["app_name"]
        LOG_LEVEL = kwargs["log"]["level"]

        def create_session(
            master: Optional[str] = "local[*]", app_name: Optional[str] = "myapp"
        ) -> SparkSession:
            """Creates a spark session.

            Parameters
            ----------
            master : String
                Contains the master node details
            app_name : String
                Name of the app that will be displayed in the spark UI

            Returns
            -------
            _type_
                _description_
            """
            spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
            return spark

        def get_settings(spark: SparkSession) -> None:
            """_summary_

            Parameters
            ----------
            spark : SparkSession
                _description_
            """
            # print(spark.sparkContext.getConf().getAll())
            pass

        def set_logging(spark: SparkSession, log_level: Optional[str] = None) -> None:
            """_summary_

            Parameters
            ----------
            spark : SparkSession
                _description_
            log_level : Optional[str], optional
                _description_, by default None
            """
            spark.sparkContext.setLogLevel(log_level) if log_level else None

        spark = create_session()
        set_logging(spark, LOG_LEVEL)
        get_settings(spark)
        return spark

    def open_json(self, filepath: str) -> dict:
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
            with open(filepath, "r") as f:
                data = json.load(f)
            return data

    def getFileExtension(self, filepath: str) -> str:
        """_summary_

        Parameters
        ----------
        filepath : str
            _description_

        Returns
        -------
        str
            _description_
        """
        if isinstance(filepath, str) and os.path.exists(filepath):
            file_name, file_extension = os.path.splitext(filepath)
            return file_extension[1:] if file_extension else None

    def list_directory(self, directory: str, pattern: Optional[str] = None) -> list:
        """_summary_

        Parameters
        ----------
        directory : str
            _description_

        Returns
        -------
        list
            _description_
        """

        def recursive_file_list(directory: str) -> list:
            """_summary_

            Parameters
            ----------
            directory : str
                _description_

            Returns
            -------
            list
                _description_
            """
            if os.path.exists(directory):
                filelist = []
                for dir_path, dir_name, filenames in os.walk(directory):
                    for filename in filenames:
                        filelist.append(f"{dir_path}/{filename}")
                return filelist

        def filter_files(filelist: list, pattern: str) -> list:
            """_summary_

            Parameters
            ----------
            filelist : list
                _description_
            pattern : str
                _description_

            Returns
            -------
            list
                _description_
            """
            return [x for x in filelist if re.search(rf"{pattern}", x)]

        file_list = recursive_file_list(directory)
        return (
            file_list
            if pattern is None
            else filter_files(file_list, pattern)
            if pattern != ""
            else None
        )

    def import_data(
        self, spark: SparkSession, datapath: str, pattern: Optional[str] = None
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

        def file_or_directory(datapath: str) -> str:
            """_summary_

            Parameters
            ----------
            datapath : _type_
                _description_

            Returns
            -------
            str
                _description_
            """
            if isinstance(datapath, str) and os.path.exists(datapath):
                if os.path.isfile(datapath):
                    return "file"
                elif os.path.isdir(datapath):
                    return "dir"

        def open_directory(
            spark: SparkSession,
            get_unique_file_extensions: Callable,
            datapath: str,
            pattern: Optional[str] = None,
        ) -> None:
            """_summary_

            Parameters
            ----------
            datapath : str
                _description_
            pattern : Optional[str], optional
                _description_, by default None
            """

            print("Inside Open Directory")
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = SparkClass(self.conf).list_directory(datapath, pattern)
                file_type = get_unique_file_extensions(filelist)
                if file_type:
                    print(filelist, file_type)
                    df = SparkClass(self.conf).create_dataframe(
                        spark, filelist, file_type
                    )
                    return df

        def open_file(filepath: str) -> None:
            """_summary_

            Parameters
            ----------
            filepath : str
                _description_
            """
            if isinstance(filepath, str) and os.path.exists(filepath):
                filelist = [filepath]
                file_type = SparkClass(self.conf).getFileExtension(filepath)
                print("file_type here - " + file_type + "\n")
                df = SparkClass(self.conf).create_dataframe(spark, filelist, file_type)
                return df

        def get_unique_file_extensions(filelist: list) -> list:
            """_summary_

            Parameters
            ----------
            filelist : list
                _description_

            Returns
            -------
            list
                import_data
            """
            print("Inside unique file extensions")
            if isinstance(filelist, list) and len(filelist) > 0:
                file_extensions = list(set(os.path.splitext(x)[1] for x in filelist))
                return file_extensions[0][1:] if len(file_extensions) == 1 else None

        path_type = file_or_directory(datapath)
        return (
            open_directory(spark, get_unique_file_extensions, datapath, pattern)
            if path_type == "dir"
            else open_file(datapath)
            if path_type == "file"
            else None
        )

    def create_dataframe(
        self, spark: SparkSession, filelist: list, filetype: str
    ) -> DataFrame:
        """_summary_

        Parameters
        ----------
        spark : SparkSession
            import_data
        filelist : list
            _description_
        filetype : str
            _description_

        Returns
        -------
        DataFrame
            _description_
        """
        print("Inside create dataframe")

        def df_from_csv(filelist: list) -> DataFrame:
            if isinstance(filelist, list) and len(filelist):
                df = (
                    spark.read.format("csv")
                    .option("header", "true")
                    .option("mode", "DROPMALFORMED")
                    .load(filelist)
                )
                return df

        def df_from_json(filelist: list) -> DataFrame:
            if isinstance(filelist, list) and len(filelist):
                df = (
                    spark.read.format("json")
                    .option("mode", "PERMISSIVE")
                    .option("primitiveAsString", "true")
                    .load(filelist)
                )
                return df

        return (
            df_from_csv(filelist)
            if filetype == "csv"
            else df_from_json(filelist)
            if filetype == "json"
            else None
        )

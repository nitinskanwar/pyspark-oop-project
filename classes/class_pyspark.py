import json, os, re, sys
from typing import (
    Callable,
    Optional,
    Any,
)  # This has been added from Python 3 onwards to add type hints
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkContext
from delta import configure_spark_with_delta_pip, DeltaTable


class SparkClass:
    """This is a spark class that can be used to start a spark session and read data from various sources."""

    def __init__(
        self,
        conf: dict,
        debug_dir: Optional[
            str
        ] = "/Volumes/Work/pyspark/pyspark-oop-project/tmp/spark",
    ) -> None:
        self.conf = conf
        self.debug_dir = debug_dir

    def create_temp_tables(self, tuple_of_dfs: tuple) -> None:
        """_summary_

        Parameters
        ----------
        tuple_of_dfs : tuple
            _description_
        """
        # print(tuple_of_dfs)
        if isinstance(tuple_of_dfs, tuple) and len(tuple_of_dfs) == 2:
            tuple_of_dfs[1].createOrReplaceTempView(tuple_of_dfs[0])

    def export_data(self, tuple_of_dfs: tuple) -> None:
        """_summary_

        Parameters
        ----------
        tuple_of_dfs : tuple
            _description_
        """
        # print(tuple_of_dfs)
        if (
            isinstance(tuple_of_dfs, tuple)
            and len(tuple_of_dfs) == 2
            and self.conf.get("export")
        ):
            path = f"{self.conf.get('export')}/{tuple_of_dfs[0]}"
            tuple_of_dfs[1].write.format("delta").mode("overwrite").save(path)

    def debug_tables(self, table) -> None:
        def create_file_path(directory: str, filepath: str) -> str:
            """_summary_

            Parameters
            ----------
            directory : str
                _description_
            filepath : str
                _description_
            """
            if isinstance(filepath, str):
                direct = f"{directory}/tables"
                return (direct, f"{direct}/{filepath}")

        def create_content(table) -> dict:
            content = {}
            content["table"] = table._asdict()
            content["dir.table"] = dir(table)
            return json.dumps(content, sort_keys=False, indent=4, default=str)

        paths = create_file_path(self.debug_dir, table.name)
        SparkClass(conf={}).debug_create_file(paths, create_content(table))

    def debug_df(self, df: DataFrame, file_name: str) -> None:
        """_summary_

        Parameters
        ----------
        df : DataFrame
            _description_
        file_name : str
            _description_
        """

        def create_file_path(directory: str, filepath: str) -> str:
            """_summary_

            Parameters
            ----------
            directory : str
                _description_
            filepath : str
                _description_
            """
            if isinstance(filepath, str):
                direct = f"{directory}/dataframes"
                return (direct, f"{direct}/{filepath}")

        def df_to_string(df: DataFrame) -> str:
            return df._jdf.schema().treeString()

        def create_content(df: DataFrame) -> dict:
            content = {}
            content["count"] = df.count()
            content["schema"] = json.loads(df.schema.json())
            return json.dumps(content, sort_keys=False, indent=4, default=str)

        paths = create_file_path(self.debug_dir, file_name)
        SparkClass(conf={}).debug_create_file(paths, create_content(df))

    def debug_create_file(self, paths: tuple, content: dict) -> None:
        """_summary_

        Parameters
        ----------
        df : DataFrame
            _description_
        file_name : str
            _description_
        """

        def make_dir(directory: str) -> None:
            """_summary_

            Parameters
            ----------
            directory : str
                _description_
            """
            if not os.path.exists(directory):
                os.makedirs(directory)

        def remove_file(filepath: str) -> None:
            if os.path.exists(filepath):
                os.remove(filepath)

        def create_file(filepath: str, content: Any) -> None:
            with open(filepath, "a") as f:
                f.write(content)

        directory = paths[0]
        filepath = paths[1]
        make_dir(directory)
        remove_file(filepath)
        create_file(filepath, content)

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

        def create_builder(
            master: str, app_name: str, config: dict
        ) -> SparkSession.Builder:
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
            builder = SparkSession.builder.master(master).appName(app_name)
            return create_delta_lake(builder, config)

        def create_delta_lake(
            builder: SparkSession.Builder, config: dict
        ) -> SparkSession.Builder:
            if (
                isinstance(builder, SparkSession.Builder)
                and config.get("deltalake") is True
            ):
                builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                ).config(
                    "spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0"
                )
                return configure_spark_with_delta_pip(builder)
            else:
                return builder

        def create_session(builder: SparkSession.Builder) -> SparkSession:
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
            if isinstance(builder, SparkSession.Builder):
                return builder.getOrCreate()

        def get_settings(spark: SparkSession) -> None:
            """_summary_

            Parameters
            ----------
            spark : SparkSession
                _description_
            """
            # print(spark.sparkContext.getConf().getAll())
            c = {}
            c["spark.version"] = spark.version
            c["spark.context"] = spark.sparkContext.getConf().getAll()
            content = json.dumps(c, sort_keys=False, indent=4, default=str)
            SparkClass(conf={}).debug_create_file(
                (f"{self.debug_dir}/config", f"{self.debug_dir}/config/spark_session"),
                content,
            )

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

        MASTER = kwargs.get("spark_conf", {}).get("master", "local[*]")
        APP_NAME = kwargs.get("spark_conf", {}).get("app_name", "myapp")
        CONFIG = kwargs.get("config")
        LOG_LEVEL = kwargs.get("log", {}).get("level", "INFO")

        builder = create_builder(MASTER, APP_NAME, CONFIG)
        spark = create_session(builder)
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

            # print("Inside Open Directory")
            if isinstance(datapath, str) and os.path.exists(datapath):
                filelist = SparkClass(self.conf).list_directory(datapath, pattern)
                file_type = get_unique_file_extensions(filelist)
                if file_type:
                    # print(filelist, file_type)
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
                # print("file_type here - " + file_type + "\n")
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
            # print("Inside unique file extensions")
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
        # print("Inside create dataframe")

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

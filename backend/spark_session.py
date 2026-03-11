from pyspark.sql import SparkSession

_spark: SparkSession | None = None


def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("SparkDungeon")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def stop_spark() -> None:
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None

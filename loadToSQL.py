import json
from pathlib import Path

import kagglehub
from pyspark.sql import SparkSession

JDBC_CONNECTOR = 'mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar'

def init_spark():
    global spark
    spark = (
        SparkSession.builder
            .config("spark.driver.host", "localhost")
            .config("spark.driver.memory", "4g")
            .config("spark.driver.cores", 5)
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", 5)
            # .config("spark.executor.instances", 3)
            .config("spark.jars", JDBC_CONNECTOR)
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ReservedCodeCacheSize=512m")
            .appName('instacart')
            .getOrCreate()
    )
    return spark


def csvToSparkDf(folder):
    global spark
    folder = Path(folder)
    kwargs = dict(header=True, quote='"', escape='"', inferSchema=True)
    dfs = {
        file: spark.read.csv(str(file), **kwargs)
        for file in folder.glob('*.csv')
    }
    return dfs

def load_credentails(cfg_path='sql_cfg.json'):
    with open(cfg_path) as f:
        return json.load(f)

def set_jdbc_creds(stream, credentials, tblname=None):
    stream = (
        stream
            .format('jdbc')
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", f"jdbc:mysql://{credentials['url']}")
            .option("user", credentials['user'])
            .option("password", credentials['password'])
    )
    if tblname:
        stream = stream.option("dbtable", f"`{credentials['schema']}`.`{tblname}`")
    return stream

def write_dfs(dfs: dict, credentials: dict):
    for f, df in dfs.items():
            set_jdbc_creds(df.write, credentials, f.stem).mode("overwrite").save()


if __name__ == '__main__':
    # Download latest version
    path = kagglehub.dataset_download("psparks/instacart-market-basket-analysis")
    print("Path to dataset files:", path)

    init_spark()

    dfs = csvToSparkDf(path)
    for f, df in dfs.items():
        print(f.stem)
        df.printSchema()

    write_dfs(dfs, load_credentails())

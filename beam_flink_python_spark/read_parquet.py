from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime

# Get spark session

spark = SparkSession.builder.appName('convert-files').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
spark.conf.set('spark.sql.debug.maxToStringFields', 100000)

def write_parquet():
    try:
        path  = '/home/isaraujo@raiadrogasil.com/Documentos/flink-code/output/part-00000-721b29da-01a1-4ec8-bc7f-f378dcd5509f-c000.snappy.parquet'
        df  = spark.read.parquet(path)
        df.printSchema()
        df.select('payload.after.CD_CANAL_VENDA','payload.after.DS_CANAL_VENDA').show(20, False)
        df.count()

    except FileNotFoundError:
        print('Arquivo não encontrado...')
    except PermissionError:
        print('permissao negada para escrever...')

if __name__ == "__main__":
    write_parquet()

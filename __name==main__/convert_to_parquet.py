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
        path  = '/home/isaraujo@raiadrogasil.com/Documentos/flink-code/input/A_RAIABD-TB_CANAL_VENDA.json'
        df_read = spark.read.json(path)
        df_read.repartition(1).write.mode('append').parquet('/home/isaraujo@raiadrogasil.com/Documentos/flink-code/output/')
    except FileNotFoundError:
        print('Arquivo não encontrado...')
    except PermissionError:
        print('permissao negada para escrever...')

if __name__ == "__main__":
    write_parquet()

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
        path  = '/home/isaraujo@raiadrogasil.com/Downloads/part-00000-c122e72f-e591-4623-9fcc-c5daaa5aaa64.c000.snappy.parquet'
        df_read = spark.read.parquet(path)
        df_write = df_read.repartition(1).write.mode('append').parquet('/home/isaraujo@raiadrogasil.com/Documentos/flink-code/output/')
        cont = 0

        while (cont < 5000000000):
            df_write = df_read.repartition(1).write.mode('append').parquet('/home/isaraujo@raiadrogasil.com/Documentos/flink-code/output/')
            cont += 1
    except FileNotFoundError:
        print('Arquivo não encontrado...')
    except PermissionError:
        print('permissao negada para escrever...')

if __name__ == "__main__":
    write_parquet()

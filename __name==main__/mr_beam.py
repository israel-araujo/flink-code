import grpc
import json
import apache_beam as beam
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io.iobase import RangeTracker
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()



def run():
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_version=1.13.0",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"
    ])

with beam.Pipeline(options=options) as p:

       data = p  | beam.io.parquetio.ReadFromParquet(p_options['s3://raiadrogasil-datalake-dev-us-east-1-109196921142-analytics/analytics/a_raiabd/tb_autorizacao_pbm/2014-06-13/058f6125-c6a5-4c9f-ac5b-130abc1ec909-0_0-34-4879_20210818214038.parquet'])
       #output = data | beam.ParDo(WordExtractionDoFn()) , schema=schema
       output    | beam.io.parquetio.WriteToParquet(p_options['s3://kubernets-flink-poc/tb_autorizacao_pbm'])
        

    )

if __name__ == "__main__":
    run()






import grpc
import json
import apache_beam as beam
import pyarrow as pa
import logging
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io.iobase import RangeTracker
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()



def run():
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_version=1.13.0",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"
    ])

with beam.Pipeline(options=options) as p:
    (p
        | 'Read from S3' >> beam.io.ReadFromText('s3://kubernets-flink-poc/A_RAIABD-TB_CANAL_VENDA.json')
        | 'Write to S3'  >> beam.io.WriteToText('s3://raiadrogasil-datalake-dev-us-east-1-109196921142-transient/bigFileFlinkBeam/A_RAIABD-TB_CANAL_VENDA', file_name_suffix='.json')
        

    )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()




s3://kubernets-flink-poc/20210723204157_1_A_RAIABD-TB_CANAL_VENDA_2G.json


        | 'Write to S3'  >> beam.io.WriteToText('s3://kubernets-flink-poc/A_RAIABD-TB_NF_DEL', file_name_suffix='.json',append_trailing_newlines=True,num_shards=3)


from pyspark.sql.types import *


path = 's3://raiadrogasil-datalake-dev-us-east-1-109196921142-transient/carga_qa/20210720182234_1_A_RAIABD-TB_PRODUTO.json'

df = spark.read.json(path)




with beam.Pipeline() as p:
  records = p | 'Read' >> beam.Create(
      [{'name': 'foo', 'age': 10}, {'name': 'bar', 'age': 20}]
  )
  _ = records | 'Write' >> beam.io.WriteToParquet('myoutput', pyarrow.schema([('name', pyarrow.binary()), ('age', pyarrow.int64())])
  )



  https://medium.com/analytics-vidhya/apache-beam-an-easy-guide-172304900014
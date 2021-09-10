import logging
import apache_beam as beam
import pyarrow as pa
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import CombinePerKey, GroupBy, GroupByKey, Map
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()


schema = pa.schema([
    ('cd_canal_venda', pa.int64()),
    ( 'ds_canal_venda', pa.string()),
    ( 'dt_process_stage', pa.date64())
    ])


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

    | 'Read Parquet'  >> beam.io.parquetio.ReadFromParquet('s3://raiadrogasil-datalake-dev-us-east-1-109196921142-stage/kafka/stage_db/tb_canal_venda/partition=2021-08-18/part-00000-c122e72f-e591-4623-9fcc-c5daaa5aaa64.c000.snappy.parquet')
    | 'write Parquet' >> beam.io.parquetio.WriteToParquet('s3://kubernets-flink-poc/canal_venda',schema=schema,file_name_suffix='.parquet')


    )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

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
           | 'Read Json'  >> beam.io.parquetio.ReadFromParquet('/home/israel/Documents/beam/modulos/input/part-00000-c122e72f-e591-4623-9fcc-c5daaa5aaa64.c000.snappy.parquet-00000-of-00001')
           | 'write Parquet' >> beam.io.parquetio.WriteToParquet('/home/israel/Documents/beam/modulos/output/isa', pa.schema([('cd_canal_venda', pa.int64()),( 'ds_canal_venda', pa.string())]),file_name_suffix='.parquet')
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
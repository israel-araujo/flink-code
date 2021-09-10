import grpc
import json
import apache_beam as beam
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()

def run():

    """
    Only read and write json file from bucket s3.

    """
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
        | 'filter descricao' >>  beam.Filter(filtering)
        | 'aplicando funcao' >>  beam.Filter(lambda x: x['CD_CANAL_VENDA'] == 1)
        | 'Write to S3'  >> beam.io.WriteToText('s3://raiadrogasil-datalake-dev-us-east-1-109196921142-transient/bigFileFlinkBeam/TB_CANAL_VENDA_Filter', file_name_suffix='.json')
        

    )


    p.run()



filter(lambda x: x['CD_CANAL_VENDA'] == 1)


























def main(known_args):
    # with beam.Pipeline(options=options) as p:
    #     (p | 'Reading input file' >> beam.io.ReadFromText(known_args.study)
    #      | 'convert from json' >> beam.ParDo(CSVtoDict())
    #      | 'convert to json' >> beam.ParDo(d2c())
    #      | 'convert to txt' >> beam.io.WriteToText('abc.json'))

    # with beam.Pipeline(options=options) as p:
    #     (p | 'Reading input file' >> beam.io.ReadFromText(known_args.study)
    #      | 'convert from json' >> beam.Map(json.loads)
    #      | 'convert to json' >> beam.Map(json.dumps)
    #      | 'convert to txt' >> beam.io.WriteToText('abc2.json'))






    python -m apache_beam.examples.wordcount --input /home/israel/Documents/input \
                                            --output /home/israel/Documents/output \
                                            --runner FlinkRunner



state FAILED: java.lang.RuntimeException: Error received from SDK harness for instruction 10
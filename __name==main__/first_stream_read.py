import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window


def convert_kafka_record_to_dictionary(record):
    # the records have 'value' attribute when --with_metadata is given
    if hasattr(record, 'value'):
        ride_bytes = record.value
    elif isinstance(record, tuple):
        ride_bytes = record[1]
    else:
        raise RuntimeError('unknown record type: %s' % type(record))
    # Converting bytes record from Kafka to a dictionary.
    import ast
    ride = ast.literal_eval(ride_bytes.decode("UTF-8"))
    output = {
        key: ride[key]
        for key in ['latitude', 'longitude', 'passenger_count']
    }
    if hasattr(record, 'timestamp'):
        # timestamp is read from Kafka metadata
        output['timestamp'] = record.timestamp
    return output

def run():
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_version=1.13",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"
    ])

    with beam.Pipeline(options=options) as pipeline:
        
        ride_col = (
        pipeline

        | ReadFromKafka(
            consumer_config={'bootstrap.servers': ['10.1.165.35:9092']},
            topics=['A_RAIABD.TB_CANAL_VENDA'],
            with_metadata=False)
        | beam.Map(lambda record: convert_kafka_record_to_dictionary(record))
        | 'write s3 log' >> beam.io.WriteToText('s3://kubernets-flink-poc/log', file_name_suffix='.txt')
       #| beam.Map(print)

        )


        ride_col.run()




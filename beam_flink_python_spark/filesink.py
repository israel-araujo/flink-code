class ParquetSink(fileio.FileSink):
    def __init__(self,
                file_path_prefix,
                schema,
                row_group_buffer_size=64 * 1024 * 1024,
                record_batch_size=1000,
                codec='none',
                use_deprecated_int96_timestamps=False,
                file_name_suffix='',
                num_shards=0,
                shard_name_template=None,
                mime_type='application/x-parquet'):
        self._inner_sink = beam.io.parquetio._create_parquet_sink(
            file_path_prefix,
            schema,
            codec,
            row_group_buffer_size,
            record_batch_size,
            use_deprecated_int96_timestamps,
            file_name_suffix,
            num_shards,
            shard_name_template,
            mime_type
        )
        self._codec = codec
        self._schema = schema
        self._use_deprecated_int96_timestamps = use_deprecated_int96_timestamps

    def open(self, fh):
        self._pw = pyarrow.parquet.ParquetWriter(
            fh,
            self._schema,
            compression=self._codec,
            use_deprecated_int96_timestamps=self._use_deprecated_int96_timestamps)

    def write(self, record):
        self._inner_sink.write_record(self._pw, record)

    def flush(self):
        if len(self._inner_sink._buffer[0]) > 0:
            self._inner_sink._flush_buffer()
        if self._inner_sink._record_batches_byte_size > 0:
            self._inner_sink._write_batches(self._pw)

        self._pw.close()



   def parquet_compatible_filenaming(suffix=None):
   
    def _inner(window, pane, shard_index, total_shards, compression, destination):
        return fileio.destination_prefix_naming(suffix )(
            window, pane, shard_index, total_shards, compression, destination).replace(":", ".")

    return _inner


def get_parquet_pipeline(pipeline_options, input, output):
    
    
    
    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p 
                    | 'Read' >> beam.io.ReadFromParquet(file_pattern=input)
                    | 'Transform' >> beam.Map(lambda x: { 'some_key': x['some_key'], 'raw': x})
                    | 'Write to Parquet' >> fileio.WriteToFiles(
                                                path=str(output),
                                                destination=lambda x: x["some_key"],
                                                sink=lambda x: ParquetSink(
                                                                    file_path_prefix=output,
                                                                    file_name_suffix=".parquet",
                                                                    codec="snappy",
                                                                    schema=pyarrow.schema([
                                                                        pyarrow.field("some_key", pyarrow.string()),
                                                                        pyarrow.field("raw", pyarrow.string())
                                                                    ])),
                                                    file_naming=parquet_compatible_filenaming(suffix=".parquet")
                                                )
                )

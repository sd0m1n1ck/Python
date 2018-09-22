import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam import ParDo, PTransform, DoFn, WindowInto
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import TimestampedValue, FixedWindows
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema

def table_field(name, type):
    name_field = TableFieldSchema()
    name_field.name = name
    name_field.type = type
    return name_field

def table_schema():
    """
    The schema of the table where we'll be writing the output data
    """
    table_schema = TableSchema()
    table_schema.fields = [
        table_field('data', 'STRING')
    ]
    return table_schema

def main(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic',
                        type=str,
                        default='',
                        help='Topic to subscribe to (use either this parameter or --input but not both).')

    parser.add_argument('--output_dataset',
                        type=str,
                        default='',
                        help='The BigQuery dataset name where to write all the data.')

    parser.add_argument('--output_table_name',
                        type=str,
                        default='',
                        help='The BigQuery table name where to write all the data.')


    args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (p | 'ReadMessages'           >> ReadFromPubSub(args.topic)             
           | 'FormatRecord'           >> beam.Map(lambda element: {"data": element})  
#          | "PrintBeforeInsert"      >> beam.Map(lambda record: print str(element))
           | 'WriteDataElementBQ'     >> WriteToBigQuery(                                                        
                     args.output_table_name,
                     args.output_dataset,
                     options.get_all_options().get("project"),
                     table_schema(),
                     BigQueryDisposition.CREATE_IF_NEEDED,
                     BigQueryDisposition.WRITE_APPEND
                 )
           )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(sys.argv)

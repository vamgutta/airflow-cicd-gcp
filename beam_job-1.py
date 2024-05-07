# import modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

#define variables 
INPUT_PATH = "gs://landing-bkt-630/sampledata.csv"

#define the pipeline options
beam_options = PipelineOptions(
    runner = 'DataflowRunner',
    project = 'morning-bootcamp-630',
    job_name = 'pipeline1',
    region = 'us-east1',
    temp_location = "gs://landing-bkt-630/temp23"
)

# define fucntions/class
class transformData(beam.DoFn):
    def process(self, element):
        id,date,item,price,quantity = element.split(",")
        total_price = float(price)*int(quantity)
        org_code = "KHIT2036"
        return [{'id':int(id),'date':date,'item':item,'price':price,'quantity':quantity,'total_price':total_price,'org_code':org_code}]

#define the pipeline object
with beam.Pipeline(options = beam_options) as pipeline:
    output = (
        pipeline
        | "read from gcs" >> beam.io.ReadFromText(INPUT_PATH, skip_header_lines=1)
        | "transform the data" >> beam.ParDo(transformData())
        | "write to bq" >> beam.io.WriteToBigQuery(
            table = "morning-bootcamp-630:INSIGHT_DS.gcs_bq_pipeline_table",
            schema= "id:INTEGER,date:STRING,item:STRING,price:FLOAT,quantity:INTEGER,total_price:FLOAT,org_code:STRING",
            write_disposition= beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition= beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location= "gs://landing-bkt-630/writeData/TEMP345"
        )
    )
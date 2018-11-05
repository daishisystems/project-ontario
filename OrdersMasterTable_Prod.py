 

from __future__ import absolute_import

import argparse
import logging
import re
import time
import ast
import datetime

#from user_agents import parse
from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io import WriteToText
#from apache_beam.options.pipeline_options import StandardOptions
import apache_beam.transforms.window as window
#from apache_beam.options.pipeline_options import SetupOptions


import sys
#from user_agents import parse
from ua_parser import user_agent_parser

sys.dont_write_bytecode = True

def run():
  """Main entry point; defines and runs the wordcount pipeline."""

  

        
  
class LoadUserAgent(beam.DoFn):
    def process(self, element):
        from ua_parser import user_agent_parser
        """
        Takes a tweet as input, search and return its content 
        and hashtag as a dictionary.
        """
        
      
        orders_json = json.loads(element)
        
        useragent = {}
        
        output = {}
        useragent_dict = {}
        output['Pipeline_EndTime'] = str(datetime.datetime.utcnow())
        
        
        if 'userAgent' in orders_json:
        
            parsed_string = user_agent_parser.Parse(str(orders_json['userAgent']))  
            output['OrderCode'] = orders_json['OrderCode'] 
            output['useragent_OS'] = parsed_string['os']['family'] #element['element']
            output['useragent_device_type'] = parsed_string['device']['family'] #element['element']
            output['useragent_device_brand'] = parsed_string['device']['brand'] #element['element']
            output['useragent_device_model'] = parsed_string['device']['model'] #element['element']
            output['useragent_browser'] = parsed_string['user_agent']['family'] #element['element']
            
            output['useragent'] = orders_json['userAgent'] 
        
        #logging.info('{}'.format(result))
        yield output        
        
class LoadOrderMessage(beam.DoFn):
    def process(self, element):
        from ua_parser import user_agent_parser
        """
        Takes a tweet as input, search and return its content 
        and hashtag as a dictionary.
        """
        orders_json = json.loads(element)
        
        useragent = {}
        useragent_dict = {}
        
        useragent_dict = ast.literal_eval(str(orders_json))
        useragent_dict['Pipeline_EndTime'] = str(datetime.datetime.utcnow())
        
        if 'userAgent' in orders_json:
            parsed_string = user_agent_parser.Parse(str(orders_json['userAgent']))
             
            
            useragent_dict['useragent_OS'] = parsed_string['os']['family'] #element['element']
            useragent_dict['useragent_device_type'] = parsed_string['device']['family'] #element['element']
            useragent_dict['useragent_device_brand'] = parsed_string['device']['brand'] #element['element']
            useragent_dict['useragent_device_model'] = parsed_string['device']['model'] #element['element']
            useragent_dict['useragent_browser'] = parsed_string['user_agent']['family'] #element['element']
            
        
        #logging.info('{}'.format(result))
        yield useragent_dict


        
class ParseJsonMessage(beam.PTransform):
    def expand(self, pcoll):
        return(
            pcoll
            | 'Decode' >> beam.Map(lambda string: string.decode('utf8', 'ignore'))
            | 'ExtractHashtags' >> beam.ParDo(LoadOrderMessage())
           
        )
        
        
        
class ExtractUSerAgent(beam.PTransform):
    def expand(self, pcoll):
        return(
            pcoll
            | 'Decode' >> beam.Map(lambda string: string.decode('utf8', 'ignore'))
            | 'ExtractHashtags' >> beam.ParDo(LoadUserAgent())
           
        )
        
        
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', required=True,
        help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument(
        '--output_table', required=True,
        help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
            'or DATASET.TABLE.'))  
    parser.add_argument(
        '--output_table_UserAgent', required=True,
        help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
            'or DATASET.TABLE.'))  
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    
    
    
    with beam.Pipeline(options =pipeline_options ) as p:
         
        lines = (
            p
         
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(known_args.input_topic))      
           # | 'ParsePubSub' >> beam.Map(lambda s:  json.loads(s)))     
          
        
        
        output_Master = (lines              
              |'OrdersMaster:totablerow' >> ParseJsonMessage()
              | 'WriteToBigQuery Order Master' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                #schema=bq_schema_string,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
                
                
        
             
        output_Useragent = (lines              
              |'UserAgent:totablerow' >> ExtractUSerAgent()
              |'WriteToBigQuery Useragent' >> beam.io.WriteToBigQuery(
                known_args.output_table_UserAgent,
                #schema=bq_schema_string,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
                
                
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
  
   
  
  
   
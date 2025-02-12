import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from TEL import TEL

# load mimic_3.json as a dictionary
import json
def load_mimic_3_config():
  with open(config_file.data_config_file) as f:
    mimic_3_config = json.load(f)
  def convert_to_lowercase(obj):
    if isinstance(obj, dict):
      return {
        k.lower(): (
            v if k == "file_name" 
            else convert_to_lowercase(v)
        ) for k, v in obj.items()
      }
    elif isinstance(obj, list):
      return [convert_to_lowercase(elem) for elem in obj]
    elif isinstance(obj, str):
      return obj.lower()
    else:
      return obj
  
  return convert_to_lowercase(mimic_3_config)

def import_mimic_3():
  mimic_3_config = load_mimic_3_config()

  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel = TEL(mongo_url, db_name)

  tel.drop_tel_db()

  mimic_tables = ["PATIENTS","ADMISSIONS","D_ICD_DIAGNOSES","ICUSTAYS","DIAGNOSES_ICD"]
  mimic_tables = [x.lower() for x in mimic_tables]
  foreign_tables = ["ADMISSIONS","D_ICD_DIAGNOSES"]
  mimic_tables = [x.lower() for x in mimic_tables]

  for table in mimic_tables:
    print("Importing table " + table)
    tel_record_docs = []
    tel_event_record_docs = []
    if table in foreign_tables:
      is_foreign_reocrd = True
    else:
      is_foreign_reocrd = False

    table_config = mimic_3_config[table]
    file_name = table_config["file_name"]
    primary_key = table_config["primary_key"]
    foreign_keys = table_config["foreign_keys"]
    time_fields = table_config["time_fields"]
    try:
      event_defs = table_config["event_defs"]
    except KeyError:
      event_defs = []

    collection = table.lower()
    with open(config_file.ehr_data_folder + file_name) as f:
      lines = f.readlines()
      header = lines[0].strip().split(",")
      header = [x.lower() for x in header]
      if "subject_id" in header:
        has_ptid = True
      else:
        has_ptid = False

      line_count = 0
      for line in lines[1:]:
        line_count += 1
        values = line.strip().split(",")
        record = {}
        for i in range(len(header)):
          record[header[i]] = values[i]
        if has_ptid:
          ptid = record["subject_id"]
          # remove subject_id from record
          record.pop("subject_id")
        else:
          ptid = None
        record_doc,event_record_docs = tel.build_tel_record(collection, ptid, record, primary_key, foreign_keys, time_fields, is_foreign_reocrd, event_defs)
        tel_record_docs.append(record_doc)
        tel_event_record_docs.extend(event_record_docs)
    tel.import_cde_records(tel_record_docs)
    # tel.import_event_records(tel_event_record_docs)

  tel.create_events_in_mongo()


if __name__ == '__main__':
  import_mimic_3()
  



        
      

        


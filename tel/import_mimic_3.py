import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time
import csv

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from TEL import TEL
from OMOP import OMOP

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

mimic_3_config = load_mimic_3_config()
mongo_url = config_file.mongo_url
db_name = config_file.tel_db_name
tel = TEL(mongo_url, db_name)

def import_mimic_3():
  tel.drop_tel_db()

  data_tables = ["DIAGNOSES_ICD", "PROCEDURES_ICD", "PROCEDUREEVENTS_MV", "CPTEVENTS", "CHARTEVENTS","DRGCODES", "PRESCRIPTIONS", "INPUTEVENTS_CV", "INPUTEVENTS_MV", "LABEVENTS", "MICROBIOLOGYEVENTS", "OUTPUTEVENTS"]
  data_tables = [x.lower() for x in data_tables]
  foreign_tables = ["ADMISSIONS", "ICUSTAYS", "D_ICD_DIAGNOSES", "D_LABITEMS", "D_ITEMS", "D_ICD_PROCEDURES", "CAREGIVERS"]
  foreign_tables = [x.lower() for x in foreign_tables]

  mimic_tables = ["patients"] + foreign_tables + data_tables

  for table in mimic_tables:
    print("Importing table " + table)
    tel_record_docs = []
    tel_event_record_docs = []
    if table in foreign_tables:
      is_foreign_record = True
    else:
      is_foreign_record = False

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
    with open(config_file.ehr_data_folder + file_name, 'r', encoding='utf-8') as f:
      csv_reader = csv.reader(f)
      header = next(csv_reader)
      header = [x.lower() for x in header]
      if "subject_id" in header:
        has_ptid = True
      else:
        has_ptid = False

      line_count = 0
      for values in csv_reader:
        line_count += 1
        record = {}
        for i in range(len(header)):
          record[header[i]] = values[i]
        if has_ptid:
          ptid = record["subject_id"]
          # remove subject_id from record
          record.pop("subject_id")
        else:
          ptid = None
        record_doc,event_record_docs = tel.build_tel_record(collection, ptid, record, primary_key, foreign_keys, time_fields, is_foreign_record, event_defs)
        tel_record_docs.append(record_doc)
        tel_event_record_docs.extend(event_record_docs)
    tel.import_cde_records(tel_record_docs)
    tel.import_event_records(tel_event_record_docs)

  tel.create_events_in_mongo()
  tel.create_indices()

  # build TEL EFCFCd
  # load ptid list from event_records
  ptid_list = tel.get_ptid_list()
  total_event_count = 0
  for i, ptid in enumerate(ptid_list):
    pt_data = []
    print(f"ptid:{ptid} {i}/{len(ptid_list)}")
    docs = tel.tel_db["event_records"].find({"ptid": ptid})
    for doc in docs:
      try:
        time = doc["time"]
      except KeyError:
        print(doc)
      pt_data.append((time, doc["event_id"]))
      total_event_count += 1
    pt_data = sorted(pt_data, key=lambda x: x[0])
    tel.import_efcfcd_to_mongo_v4(ptid, pt_data)
  tel.create_fcs_indices()

  tel.build_eii()

def index_mimic_3():
  tel.create_indices()

if __name__ == '__main__':
  import_mimic_3()
  



        
      

        


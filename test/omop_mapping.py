import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL import TEL
from tel.TEL_CDE import TEL_CDE
from tel.OMOP import OMOP

mongo_url = config_file.mongo_url
db_name = config_file.tel_db_name
tel_cde = TEL_CDE(mongo_url, db_name)
omop = OMOP(mongo_url, "omop")
tel = TEL(mongo_url, db_name)

def build_mapping():
  mapping_file_path = config_file.mapping_file_path
  tel_cde.create_omop_mapping(mapping_file_path,omop)

def save_mapping_to_csv():
  import csv
  mapping_file_path = config_file.omop_data_folder + "/omop_mimic3_mapping.csv"

  print("Building mapping...")
  rows = []
  docs = tel_cde.tel_db["omop_cde_mapping"].find()
  for doc in docs:
    cde_id = doc["cde_id"]
    omop_concept_id = doc["omop_concept_id"]
    rows.append({"cde_id": cde_id, "omop_concept_id": omop_concept_id})
  for row in rows:
    # get cde info and imop info
    cde_id = row["cde_id"]
    cde_info = tel_cde.tel_db["cde"].find_one({"id": cde_id})
    row["cde_collection"] = cde_info["collection"]
    row["cde_field"] = cde_info["field"]
    row["cde_value"] = cde_info["str"]
    row["cde_count"] = cde_info["count"]
    omop_concept_id = row["omop_concept_id"]
    omop_info = omop.omop_db["concept"].find_one({"concept_id": omop_concept_id})
    row["omop_domain_id"] = omop_info["domain_id"]
    row["omop_vocabulary_id"] = omop_info["vocabulary_id"]
    row["omop_concept_code"] = omop_info["concept_code"]
    row["omop_concept_name"] = omop_info["concept_name"]
  # sort by count
  rows = sorted(rows, key=lambda x: x["cde_count"], reverse=True)
  # write to csv
  print(f"Writing to {mapping_file_path}")
  with open(mapping_file_path, mode='w') as file:
    writer = csv.DictWriter(file, fieldnames=["cde_id", "cde_collection", "cde_field", "cde_value", "cde_count", "omop_concept_id", "omop_domain_id", "omop_vocabulary_id", "omop_concept_code", "omop_concept_name"])
    writer.writeheader()
    for row in rows:
      writer.writerow(row)

def get_events_by_omop():
  # 401.9 Unspecified essential hypertension
  omop_concept_id = "44821949"
  # get cdes
  docs = tel_cde.tel_db["omop_cde_mapping"].find({"omop_concept_id": omop_concept_id})
  cde_ids = [doc["cde_id"] for doc in docs]
  print(f"Found {len(cde_ids)} cdes")
  print(cde_ids)

  # search event by cde
  event_list = tel.search_event_by_cde([cde_ids])
  event_id_list = [x["id"] for x in event_list]

  print(f"Found {len(event_id_list)} events")
  print(event_id_list)
  
  

if __name__ == "__main__":
  # build_mapping()
  # save_mapping_to_csv()

  get_events_by_omop()
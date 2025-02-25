import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL_CDE import TEL_CDE
from tel.OMOP import OMOP

mongo_url = config_file.mongo_url
db_name = config_file.tel_db_name
tel_cde = TEL_CDE(mongo_url, db_name)
omop = OMOP(mongo_url, "omop")

def test_omop_concept_search():
  concept_name = "aspirin"

  print("search omop concept by name")
  start_time = time.time()
  concept_docs = omop.concept_name_search(concept_name, "NDC")
  for doc in concept_docs:
    print(doc)
  end_time = time.time()
  print(f"Running time: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")

  print("fuzzy search omop concept by name")
  start_time = time.time()
  concept_docs = omop.concept_name_search(concept_name, "NDC", fuzzy=True)
  fuzzy_count = 0
  for doc in concept_docs:
    print(doc)
    fuzzy_count += 1
  end_time = time.time()
  print(f"Fuzzy search count: {fuzzy_count}")
  print(f"Running time: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")

if __name__ == "__main__":
  test_omop_concept_search()
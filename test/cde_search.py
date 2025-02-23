# test casesfor TEL_CDE functions get_cde_mongo and search_cde_mongo
import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL_CDE import TEL_CDE

def test_tel_cde_search():
  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel_cde = TEL_CDE(mongo_url, db_name)

  tel_cde.create_indices()

  term = "aspirin"
  collection = "prescriptions"
  field = "drug"

  start_time = time.time()
  print("search_cde_mongo")
  cdes = tel_cde.search_cde_mongo(term,field,collection)
  for cde in cdes:
    cde_id = cde["id"]
    doc = tel_cde.get_cde_mongo(cde_id)
    print(doc)
  end_time = time.time()
  print(f"Running time: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")

  start_time = time.time()
  print("fuzzy_search_cde_mongo")
  cdes = tel_cde.fuzzy_search_cde_mongo(term,field=field,collection=collection,limit=None)
  for cde in cdes:
    cde_id = cde["id"]
    doc = tel_cde.get_cde_mongo(cde_id)
    print(doc)
  end_time = time.time()
  print(f"Running time: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")

if __name__ == "__main__":
  test_tel_cde_search()
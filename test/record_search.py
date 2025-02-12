import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL import TEL

def test_tel_record_search():
  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel = TEL(mongo_url, db_name)

  cde_list = [2,413,426]
  cde_list = [ [2],[413,426]]
  results = tel.record_query_by_cde(cde_list)
  result_count = len(results)
  for result in results:
    print(result)
  print("Total number of records: ", result_count)

if __name__ == "__main__":
  test_tel_record_search()
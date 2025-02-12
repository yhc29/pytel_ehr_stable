import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL import TEL

def test_tel_event_query():
  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel = TEL(mongo_url, db_name)

  tel_cde = tel.tel_cde

  # case 1, dob in patients
  tel_cde = tel.tel_cde
  tcde_list = tel_cde.search_temporal_cde_mongo("dob")
  for _tcde in tcde_list:
    print(_tcde)
  tcde_id = tcde_list[0]["id"]

  event_list = tel.search_event_by_cde([], tcde_id)
  event_ids = []
  for event in event_list:
    print(event)
    event_ids.append(event["id"])
  event_records = tel.event_record_query_by_cde(event_ids)
  for event_record in event_records:
    print(event_record)

if __name__ == "__main__":
  test_tel_event_query()
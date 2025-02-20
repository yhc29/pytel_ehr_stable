import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL import TEL

def case1(tel):
  tel_cde = tel.tel_cde
  # case 1, dob in patients
  tcde_list = tel_cde.search_temporal_cde_mongo("dob")
  for _tcde in tcde_list:
    print(_tcde)
  tcde_id = tcde_list[0]["id"]

  event_list = tel.search_event_by_cde([], tcde_id)
  event_id_list = [x["id"] for x in event_list]
  for event in event_list:
    print(event)

  event_records = tel.event_record_query_by_cde(event_id_list)
  for event_record in event_records:
    print(event_record)

def case2(tel):
  tel_cde = tel.tel_cde
  # case 2, diagnosis hypertension
  cde_list = tel_cde.fuzzy_search_cde_mongo("hypertension",limit=None)
  # for _cde in cde_list:
  #   print(_cde)
  cde_id_list = [x["id"] for x in cde_list]
  # print(cde_id_list)

  # search event by cde
  event_list = tel.search_event_by_cde([cde_id_list])
  event_id_list = [x["id"] for x in event_list]
  # for event in event_list:
  #   print(event)

  # event record query by cde
  event_records = tel.event_record_query_by_cde(event_id_list)
  # print first 5 event records
  for event_record in event_records[:5]:
    print(event_record)

def test_tel_event_query():
  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel = TEL(mongo_url, db_name)

  # case1(tel)
  case2(tel)


if __name__ == "__main__":
  test_tel_event_query()
import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

import sys
sys.path.insert(0, '..')

import config.ibm as config_file
from tel.TEL import TEL
from tel.TEL_Query import TEL_Query

def case1(tel,tel_query_client):
  tel_cde = tel.tel_cde
  # event 1, dob in patients
  tcde_list = tel_cde.search_temporal_cde_mongo("dob")
  for _tcde in tcde_list:
    print(_tcde)
  tcde_id = tcde_list[0]["id"]

  event_list = tel.search_event_by_cde([], tcde_id)
  event_id_list = [x["id"] for x in event_list]
  print(f"event_id_list: {event_id_list}")
  # for event in event_list:
  #   print(event)

  # event 2, diagnosis hypertension
  collection = "d_icd_diagnoses"
  field = "short_title"
  # cde_list2 = tel_cde.fuzzy_search_cde_mongo("hypertension", field, collection)
  # for _cde in cde_list:
  #   print(_cde)
  # cde_id_list2 = [x["id"] for x in cde_list2]
  cde_id_list2 = [734054]
  # print(cde_id_list)

  # search event by cde
  # event_list2 = tel.search_event_by_cde([cde_id_list2])
  # event_id_list2 = [x["id"] for x in event_list2]
  event_id_list2 = [112]
  print(f"event_id_list2: {event_id_list2}")

  # query = [event1, event2, 180*delta_unit, "lte", True, False]
  delta_max = 100*365*24*60*60
  delta_max_op = "lte"
  tel_result = tel_query_client.efcfcd_diamond_v4_1(event_id_list, event_id_list2, delta_max, delta_max_op, False, False)
  print(f"tel_result: {len(tel_result)}")


def test_tel_temporal_query():
  mongo_url = config_file.mongo_url
  db_name = config_file.tel_db_name
  tel = TEL(mongo_url, db_name)
  tel_query_client = TEL_Query(config_file.mongo_url, db_name)

  tel.create_indices()
  tel_query_client.create_indices()


  case1(tel,tel_query_client)

if __name__ == "__main__":
  test_tel_temporal_query()
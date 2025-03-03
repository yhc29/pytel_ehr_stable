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

def build_mapping():
  mapping_file_path = config_file.mapping_file_path
  tel_cde.create_omop_mapping(mapping_file_path,omop)

if __name__ == "__main__":
  build_mapping()
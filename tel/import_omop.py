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

mongo_url = config_file.mongo_url
db_name = "omop"
omop = OMOP(mongo_url, db_name)
omop_folder = config_file.omop_data_folder

if __name__ == '__main__':
  # omop.import_omop(omop_folder)
  omop.build_indices()
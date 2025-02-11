import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

class TEL_CDE:
	def __init__(self, mongo_url, db_name):
		self.client = pymongo.MongoClient(mongo_url)
		self.tel_db = self.client[db_name]
		self.cde = {}
		self.max_id = 0

	def clear_cde(self):
		# drop collection cde
		self.tel_db["cde"].drop()

	def load_cde_from_mongo(self):
		cde = {}
		docs = self.tel_db["cde"].find()
		# {id: xx, collection: xx, field: xx, value: xx, value_type: xx, count: xx}
		for doc in docs:
			id = doc["id"]
			collection = doc["collection"]
			field = doc["field"]
			value = doc["value"]
			value_type = doc["value_type"]
			count = doc["count"]
			key = collection + "|" + field + "|" + str(value)
			cde[key] = {"id": id,"collection": collection, "field": field, "value": value, "value_type": value_type, "count": count}

		self.cde = cde
	
	def create_cde_in_mongo(self):
		self.clear_cde()
		docs = [x for x in self.cde.values()]
		self.tel_db["cde"].insert_many(docs)

		# create index
		self.tel_db["cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["cde"].create_index([("value", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("collection", pymongo.ASCENDING)], unique=True)

	def update_cde_to_mongo(self):
		for doc in self.cde.values():
			self.tel_db["cde"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
			
	
	def get_max_id(self):
		max_id = 0
		for id in self.cde:
			if self.cde[id]["id"] > max_id:
				max_id = self.cde[id]["id"]
		self.max_id = max_id

	def add_element(self, collection,field, value, value_type):
		key = collection + "|" + field + "|" + str(value)
		if key in self.cde:
			if value_type != self.cde[key]["value_type"]:
				print(f"Warning: value_type mismatch: given {value_type}, but get {self.cde[key]['value_type']}")
			self.cde[key]["count"] += 1
		else:
			self.max_id += 1
			self.cde[key] = {"id": self.max_id, "collection": collection, "field": field, "value": value, "value_type": value_type, "count": 1}

		return self.cde[key]
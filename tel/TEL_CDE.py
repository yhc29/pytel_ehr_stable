import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

class TEL_CDE:
	def __init__(self, mongo_url, db_name):
		self.client = pymongo.MongoClient(mongo_url)
		self.tel_db = self.client[db_name]
		self.cde = {}
		self.max_cde_id = 0
		self.temporal_cde = {}
		self.max_temporal_cde_id = 0

	def clear_cde(self):
		# drop collection cde
		self.tel_db["cde"].drop()
		# drop collection temporal_cde
		self.tel_db["temporal_cde"].drop()

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

		temporal_cde = {}
		docs = self.tel_db["temporal_cde"].find()
		# {id: xx, collection: xx, field: xx, type: xx, count: xx}
		for doc in docs:
			id = doc["id"]
			collection = doc["collection"]
			field = doc["field"]
			type = doc["type"]
			count = doc["count"]
			key = collection + "|" + field + "|" + type
			temporal_cde[key] = {"id": id,"collection": collection, "field": field, "type": type, "count": count}
	
	def create_cde_in_mongo(self):
		print("Creating cde in mongo")
		self.clear_cde()
		docs = [x for x in self.cde.values()]
		if len(docs) > 0:
			self.tel_db["cde"].insert_many(docs)
		else:
			print("No cde to insert")

		temporal_docs = [x for x in self.temporal_cde.values()]
		if len(temporal_docs) > 0:
			self.tel_db["temporal_cde"].insert_many(temporal_docs)
		else:
			print("No temporal_cde to insert")

		# create index
		print("Creating index for cde")
		self.tel_db["cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["cde"].create_index([("value", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("collection", pymongo.ASCENDING)], unique=True)
		self.tel_db["cde"].create_index([("str", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("collection", pymongo.ASCENDING)])
		print("Creating index for temporal_cde")
		self.tel_db["temporal_cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["temporal_cde"].create_index([("field", pymongo.ASCENDING), ("collection", pymongo.ASCENDING), ("type", pymongo.ASCENDING)], unique=True)

	def update_cde_to_mongo(self):
		for doc in self.cde.values():
			self.tel_db["cde"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
		for doc in self.temporal_cde.values():
			self.tel_db["temporal_cde"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
			
	
	def get_max_id(self):
		max_id = 0
		for id in self.cde:
			if self.cde[id]["id"] > max_id:
				max_id = self.cde[id]["id"]
		self.max_cde_id = max_id
	
	def get_max_temporal_cde_id(self):
		max_id = 0
		for id in self.temporal_cde:
			if self.temporal_cde[id]["id"] > max_id:
				max_id = self.temporal_cde[id]["id"]
		self.max_temporal_cde_id = max_id

	def add_element(self, collection, field, value, value_type,is_primary_key=False):
		key = collection + "|" + field + "|" + str(value)
		if key in self.cde:
			if value_type != self.cde[key]["value_type"]:
				print(f"Warning: value_type mismatch: given {value_type}, but get {self.cde[key]['value_type']}")
			self.cde[key]["count"] += 1
		else:
			self.max_cde_id += 1
			self.cde[key] = {"id": self.max_cde_id, "collection": collection, "field": field, "value": value, "value_type": value_type, "str": str(value).lower(), "count": 1}
			if is_primary_key:
				self.cde[key]["is_primary_key"] = True

		return self.cde[key]
	def add_temporal_element(self, collection, field, type):
		key = collection + "|" + field + "|" + type
		if key in self.temporal_cde:
			self.temporal_cde[key]["count"] += 1
		else:
			self.max_temporal_cde_id += 1
			self.temporal_cde[key] = {"id": self.max_temporal_cde_id, "collection": collection, "field": field, "type": type, "count": 1}

		return self.temporal_cde[key]
	
	def get_cde_mongo(self, id):
		doc = self.tel_db["cde"].find_one({"id": id}, {"_id": 0})
		return doc
	
	def get_cde_str_mongo(self, id):
		doc = self.tel_db["cde"].find_one({"id": id}, {"_id": 0})
		collection = doc["collection"]
		field = doc["field"]
		value = doc["value"]
		value = str(value)
		cde_str = collection + "|" + field + "|" + value

		return cde_str
	def get_temporal_cde_str_mongo(self, id):
		doc = self.tel_db["temporal_cde"].find_one({"id": id}, {"_id": 0})
		collection = doc["collection"]
		field = doc["field"]
		type = doc["type"]
		temporal_cde_str = collection + "|" + field + "|" + type

		return temporal_cde_str
	
	def search_cde_mongo(self, value, field= None, collection = None):
		query = {"value": {"$exists": True}}
		if value:
			query["value"] = value
		if field:
			query["field"] = field
		if collection:
			try:
				query["field"]
			except KeyError:
				query["field"] = {"$exists": True}
			query["collection"] = collection

		docs = self.tel_db["cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key = lambda x: x["count"], reverse = True)
		return results
	
	def search_temporal_cde_mongo(self, field, collection = None):
		query = {"field": {"$exists": True}}
		if field:
			pattern =  f".*{field}.*"
			query["field"] = {"$regex": pattern}
		if collection:
			query["collection"] = collection

		docs = self.tel_db["temporal_cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key = lambda x: x["count"], reverse = True)
		return results
	
	def fuzzy_search_cde_mongo(self, search_term, field=None, collection=None, limit=10):
		"""
		Performs fuzzy search on CDE using MongoDB's $regex
		
		Args:
			search_term (str): Term to search for
			field (str, optional): Field name to search in
			collection (str, optional): Collection name to search in
				
		Returns:
			list: Matching CDE documents sorted by count
		"""
		# Convert search term to case-insensitive regex pattern
		search_term = str(search_term).lower()
		pattern = f".*{search_term}.*"
		
		query = {
			"str": {"$regex": pattern}
		}
		
		if field:
			query["field"] = field
		if collection:
			try:
				query["field"]
			except KeyError:
				query["field"] = {"$exists": True}
			query["collection"] = collection

		# print(query)
		docs = self.tel_db["cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key=lambda x: x["count"], reverse=True)
		if limit:
			results = results[:limit]
		
		return results
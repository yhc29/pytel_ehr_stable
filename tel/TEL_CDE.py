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
		# self.tel_db["cde"].create_index([("collection", pymongo.TEXT), ("field", pymongo.TEXT), ("value", pymongo.ASCENDING)])
		self.tel_db["cde"].create_index([("collection", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("str", pymongo.ASCENDING)])
		print("Creating index for temporal_cde")
		self.tel_db["temporal_cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["temporal_cde"].create_index([("collection", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])

	def create_indices(self):
		print("Creating index for cde")
		self.tel_db["cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["cde"].create_index([("collection", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("str", pymongo.ASCENDING)])
		print("Creating index for temporal_cde")
		self.tel_db["temporal_cde"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["temporal_cde"].create_index([("collection", pymongo.ASCENDING), ("field", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])

	def update_cde_to_mongo(self):
		for doc in self.cde.values():
			self.tel_db["cde"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
		for doc in self.temporal_cde.values():
			self.tel_db["temporal_cde"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
			
	def stats(self):
		# get all collections from mongo
		docs = self.tel_db["cde"].distinct("collection")
		collections = [x for x in docs]
		print(f"collections: {collections}")
		for collection in collections:
			print(f"*********************collection: {collection}")
			# get number of values and total count in each field
			ap_stmt = [
				{"$match": {"collection": collection}},
				{"$group": {"_id": "$field", "record_count": {"$sum": "$count"}, "value_count": {"$sum": 1}}},
			]
			docs = self.tel_db["cde"].aggregate(ap_stmt)
			fields = [x for x in docs]
			# sort by value_count in ascending order
			fields = sorted(fields, key = lambda x: x["value_count"])
			
			for field in fields:
				print(f"field: {field['_id']}, value_count: {field['value_count']}, record_count: {field['record_count']}, ratio: {field['value_count']*100/field['record_count']:.2f}%")

	def create_omop_mapping(self, mapping_file_path, omop):
		# create index for omop concept collection
		print("Creating index for omop concept")
		omop.omop_db["concept"].create_index([("domain_id", pymongo.ASCENDING), ("vocabulary_id", pymongo.ASCENDING), ("concept_code", pymongo.ASCENDING)])
		# omop.omop_db["concept"].create_index([("domain_id", pymongo.ASCENDING), ("vocabulary_id", pymongo.ASCENDING), ("concept_name", pymongo.ASCENDING)])
		print("Creating index for omop concept done")

		print("Drop omop_cde_mapping")
		self.tel_db["omop_cde_mapping"].drop()
		print("Drop omop_cde_mapping done")

		mapping_docs = []
		mapping_doc_count = 0
		cde_scanned_count = 0
		# load json file
		import json
		with open(mapping_file_path, 'r') as f:
			mapping_config = json.load(f)
			for collection in mapping_config:
				for field in mapping_config[collection]:
					mapped_count_this_field = 0
					scanned_count_this_field = 0
					print(f"create_omop_mapping: collection: {collection}, field: {field}")
					omop_domain_id = mapping_config[collection][field]["omop_domain_id"]
					omop_vocabulary_id = mapping_config[collection][field]["omop_vocabulary_id"]
					mapping_field = mapping_config[collection][field]["mapping_field"]
					_mappings = mapping_config[collection][field]

					cde_docs = self.tel_db["cde"].find({"collection": collection, "field": field})
					for cde_doc in cde_docs:
						cde_scanned_count += 1
						scanned_count_this_field += 1
						cde_id = cde_doc["id"]
						cde_str = cde_doc["str"]
						if field == "icd9_code":
							if collection == "diagnoses_icd":
								cde_str = format_icd9(cde_str)
							elif collection == "procedures_icd":
								cde_str = format_icd9(cde_str, is_procedure=True)
						# find omop concept_id
						_or_stmt = []
						for _mapping_info in _mappings:
							_omop_domain_id = _mapping_info["omop_domain_id"]
							_omop_vocabulary_id = _mapping_info["omop_vocabulary_id"]
							_mapping_field = _mapping_info["mapping_field"]
							_or_stmt.append({"domain_id": _omop_domain_id, "vocabulary_id": _omop_vocabulary_id, _mapping_field: cde_str})

						docs = omop.omop_db["concept"].find({"$or": _or_stmt})
						mapped_omop_concepts = [x for x in docs]
						if len(mapped_omop_concepts) > 0:
							mapped_count_this_field += 1
						for doc in mapped_omop_concepts:
							omop_concept_id = doc["concept_id"]
							mapping_docs.append({"cde_id": cde_id, "omop_concept_id": omop_concept_id})
							mapping_doc_count += 1
							if len(mapping_docs) >= 5000:
								self.tel_db["omop_cde_mapping"].insert_many(mapping_docs)
								mapping_docs = []
								print(f"mapped_count_this_field: {mapped_count_this_field}, scanned_count_this_field: {scanned_count_this_field}")
						# else:
						# 	print(f"Warning: {cde_str} not found in omop concept")
					print(f"{field} done! mapped_count_this_field: {mapped_count_this_field}, scanned_count_this_field: {scanned_count_this_field}")
					print(f"total mapping_doc_count: {mapping_doc_count}, cde_scanned_count: {cde_scanned_count}")
				if len(mapping_docs) > 0:
					self.tel_db["omop_cde_mapping"].insert_many(mapping_docs)
					mapping_docs = []
		print(f"Import done, total mapping_doc_count: {mapping_doc_count}")
		# create index
		print("Creating index for omop_cde_mapping")
		self.tel_db["omop_cde_mapping"].create_index([("cde_id", pymongo.ASCENDING)])
		self.tel_db["omop_cde_mapping"].create_index([("omop_concept_id", pymongo.ASCENDING)])
		print("Creating index for omop_cde_mapping done")




	
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
		query = {
			"collection": {"$exists": True},
			"field": {"$exists": True},
			"str": str(value).lower()}

		if field:
			query["field"] = field
		if collection:
			query["collection"] = collection

		docs = self.tel_db["cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key = lambda x: x["count"], reverse = True)
		return results
	
	def search_temporal_cde_mongo(self, field, collection = None):
		query = {
			"collection": {"$exists": True},
			"field": {"$exists": True}}
		if field:
			pattern =  f".*{field}.*"
			query["field"] = {"$regex": pattern}
		if collection:
			query["collection"] = collection

		docs = self.tel_db["temporal_cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key = lambda x: x["count"], reverse = True)
		return results
	
	def fuzzy_search_cde_mongo(self, search_term, field=None, collection=None, limit=None):
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
			"collection": {"$exists": True},
			"field": {"$exists": True},
			"str": {"$regex": pattern}
		}
		
		if field:
			query["field"] = field
		if collection:
			query["collection"] = collection

		print(query)
		docs = self.tel_db["cde"].find(query, {"_id": 0})
		results = [x for x in docs]
		results = sorted(results, key=lambda x: x["count"], reverse=True)
		if limit:
			results = results[:limit]
		
		return results
	

def format_icd9(code, is_procedure=False):
    """
    Format ICD9 code by adding dots
    
    Args:
        code (str): ICD9 code string
        is_procedure (bool): True if procedure code, False if diagnosis code
    
    Returns:
        str: Formatted ICD9 code
    """
    # Remove existing dots and leading/trailing spaces
    code = code.replace('.', '').strip()
    
    if not code:
        return code
        
    if is_procedure:
        # Procedure codes: dot after 2nd digit
        if len(code) > 2:
            return code[:2] + '.' + code[2:]
        return code
    else:
        # Diagnosis codes: dot after 3rd digit
        if len(code) > 3:
            return code[:3] + '.' + code[3:]
        return code
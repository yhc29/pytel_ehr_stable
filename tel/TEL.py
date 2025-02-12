import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time
from tel.TEL_CDE import TEL_CDE

class TEL:
	def __init__(self, mongo_url, db_name):
		self.client = pymongo.MongoClient(mongo_url)
		self.tel_db = self.client[db_name]

		self.tel_cde = TEL_CDE(mongo_url, db_name)
		self.events = {}
		self.max_event_id = 0

		self.foreign_records = {}
	
	def drop_tel_db(self):
		self.client.drop_database(self.tel_db.name)

	def load_events_from_mongo(self):
		events = {}
		docs = self.tel_db["events"].find()
		# {id: xx, cde: [xx, xx, xx], tcde: xx, count: xx}
		for doc in docs:
			id = doc["id"]
			cde = doc["cde"]
			try:
				tcde = doc["tcde"]
			except KeyError:
				tcde = ""
			count = doc["count"]
			key = "|".join([str(x) for x in cde + [tcde]])
			if tcde:
				events[key] = {"id": id, "cde": cde, "tcde": tcde, "count": count}
			else:
				events[key] = {"id": id, "cde": cde, "count": count}

		self.events = events

	def create_events_in_mongo(self):
		self.tel_cde.create_cde_in_mongo()

		print("Creating events in mongo")
		self.tel_db["events"].drop()

		docs = [x for x in self.events.values()]
		if len(docs) > 0:
			self.tel_db["events"].insert_many(docs)
		else:
			print("No events to insert")

		# create index
		print("Creating index for events")
		self.tel_db["events"].create_index([("id", pymongo.ASCENDING)], unique=True)
		self.tel_db["events"].create_index([("cde", pymongo.ASCENDING)])

	def update_event_in_mongo(self):
		for doc in self.events.values():
			self.tel_db["events"].update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)

	def drop_records(self):
		self.tel_db["cde_records"].drop()
		self.tel_db["event_records"].drop()

	def import_cde_records(self, docs):
		if len(docs) > 0:
			self.tel_db["cde_records"].insert_many(docs)

	def import_event_records(self, docs):
		if len(docs) > 0:
			self.tel_db["event_records"].insert_many(docs)

	def build_tel_record(self, collection, ptid, record, primary_key, foreign_keys, time_fields, is_foreign_record=False, event_defs = []):
		if is_foreign_record:
			try:
				self.foreign_records[collection]
			except KeyError:
				self.foreign_records[collection] = {}
			record_primary_key = record[primary_key]
			self.foreign_records[collection][record_primary_key] = {}

		cde_record = {}
		record_doc = {"ptid": ptid, "cde": []}
		for field in record:
			value = record[field]
			if field in time_fields:
				if value:
					if not isinstance(value, datetime):
						value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
					record[field] = value
				else:
					value = None
			else:
				# check if value is a float or int or string
				if value:
					try:
						value = float(value)
						if value.is_integer():
							value = int(value)
					except ValueError:
						pass
			value_type = type(value).__name__
			if field == primary_key:
				is_primary_key = True
			else:
				is_primary_key = False
			try:
				cde_collection = foreign_keys[field]
			except KeyError:
				cde_collection = collection
			element = self.tel_cde.add_element(cde_collection, field, value, value_type,is_primary_key)
			cde_record[field] = element["id"]
			record_doc["cde"].append(element["id"])
			if is_foreign_record:
				if value_type == "datetime":
					self.foreign_records[collection][record_primary_key][field] = value
				else:
					self.foreign_records[collection][record_primary_key][field] = element["id"]

		# build events
		event_docs = []
		for event_def in event_defs:
			event_fields = event_def["fields"]
			event_foreign_fields = event_def["foreign_fields"]
			cde_id_list = [cde_record[field] for field in event_fields]
			for event_foreign_collection in event_foreign_fields:
				try:
					_event_foreign_key = event_foreign_fields[event_foreign_collection]["foreign_key"]
					_event_foreign_fields = event_foreign_fields[event_foreign_collection]["fields"]
					for field in _event_foreign_fields:
						_this_cde = self.foreign_records[event_foreign_collection][record[_event_foreign_key]]
						cde_id_list.append(_this_cde)
				except KeyError:
					print(record)
					print(f"Error: cannot get cde from {event_foreign_collection}")

			temporal_collection = event_def["temporal_collection"]
			try:
				temporal_foreign_key = event_def["temporal_foreign_key"]
			except KeyError:
				temporal_foreign_key = None
			temporal_field = event_def["temporal_field"]
			temporal_type = event_def["temporal_type"]
			if temporal_field:
				# temporal event
				if temporal_foreign_key:
					t = self.foreign_records[temporal_collection][record[temporal_foreign_key]][temporal_field]
				else:
					t = record[temporal_field]
				temporal_cde = self.tel_cde.add_temporal_element(temporal_collection, temporal_field, temporal_type)
				temporal_cde_id = temporal_cde["id"]
			else:
				# non-temporal event
				t = None
				temporal_cde_id = None
			event = self.add_event(cde_id_list, temporal_cde_id)
			event_id = event["id"]
			if t:
				event_record_doc = {"ptid": ptid, "event_id": event_id, "time": t}
			else:
				event_record_doc = {"ptid": ptid, "event_id": event_id}
			event_docs.append(event_record_doc)

		return record_doc,event_docs
	
	def add_event(self, cde__id_list, temporal_cde_id):
		keys = [str(x) for x in cde__id_list]
		if temporal_cde_id:
			keys.append(str(temporal_cde_id))
		else:
			keys.append("")
		key = "|".join(keys)
		try:
			event = self.events[key]
			event["count"] += 1
		except KeyError:
			self.max_event_id += 1
			event = {"id": self.max_event_id, "cde": cde__id_list, "tcde": temporal_cde_id, "count": 1}
			self.events[key] = event

		return event
	
	def create_indices(self):
		print("Creating indices for cde_records")
		self.tel_db["cde_records"].create_index([("ptid", pymongo.ASCENDING)])
		self.tel_db["cde_records"].create_index([("cde", pymongo.ASCENDING)])

		print("Creating indices for event_records")
		self.tel_db["event_records"].create_index([("ptid", pymongo.ASCENDING)])
		self.tel_db["event_records"].create_index([("event_id", pymongo.ASCENDING)])
		self.tel_db["event_records"].create_index([("time", pymongo.ASCENDING)])


	def record_query_by_cde(self, cde_id_list, limit = None):
		# if cde_id_list is 1-d list:
		# get all records with all of the cde_id in cde_id_list
		# if cde_id_list is 2-d list:
		# get all records with any of the cde_id in each sublist of cde_id_list
		if len(cde_id_list) == 0:
			return []
		if isinstance(cde_id_list[0], list):
			query = {"$and": [{"cde": {"$in": x}} for x in cde_id_list]}
		else:
			query = {"cde": {"$all": cde_id_list}}
		if limit:
			docs = self.tel_db["cde_records"].find(query).limit(limit)
		else:
			docs = self.tel_db["cde_records"].find(query)

		results = []
		for doc in docs:
			try:
				ptid = doc["ptid"]
			except KeyError:
				ptid = None
			cde_list = doc["cde"]
			cde_str_list = [self.tel_cde.get_cde_str_mongo(x) for x in cde_list]
			results.append({"ptid": ptid, "cde": cde_list, "cde_str": cde_str_list})
		return results




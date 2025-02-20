import pymongo
import bson
from datetime import datetime, timedelta, timezone
import time

class TEL_Query:
	def __init__(self, mongo_url, db_name):
		self.client = pymongo.MongoClient(mongo_url)
		self.tel_db = self.client[db_name]

	def benchmark_diamond_1(self, event1_list, event2_list, delta_max, delta_max_op="lt",cooccurrence=True,negation=False):
		# event_id_field_name = "event_id"
		# data_collection_name = "pt_timeline"
		# ptid_field_name = "ID"

		candidates = self.get_candidates(event1_list, event2_list)

		event_id_field_name = "event_id"
		data_collection_name = "event_reocrds"
		ptid_field_name = "ptid"

		if cooccurrence:
			delta_min_op = "gte"
		else:
			delta_min_op = "gt"

		result = []
		for pt_group_id,ptid_list in candidates.items():
			ap_stmt = [
				{"$match": {ptid_field_name: {"$in":ptid_list}, event_id_field_name: {"$in": event1_list + event2_list}}},
				{"$group": {"_id": "$" + ptid_field_name, "e1": {"$push": {"$cond": [{"$in": ["$" + event_id_field_name, event1_list]}, "$time", None]}}, "e2": {"$push": {"$cond": [{"$in": ["$" + event_id_field_name, event2_list]}, "$time", None]}}}},
				# Filter out null values from arrays
				{"$project": {
					"e1": {
							"$filter": {
									"input": "$e1",
									"as": "item",
									"cond": {"$ne": ["$$item", None]}
							}
					},
					"e2": {
							"$filter": {
									"input": "$e2",
									"as": "item",
									"cond": {"$ne": ["$$item", None]}
							}
					},
				}},
				{"$match": {"e1": {"$ne": []}, "e2": {"$ne": []}}},
				{"$unwind": "$e1"},
				{"$unwind": "$e2"},
				{"$match": {"$expr": {"$and": [{"$"+delta_min_op: [{"$subtract": ["$e2", "$e1"]}, 0]}, {"$"+delta_max_op: [{"$subtract": ["$e2", "$e1"]}, delta_max*1000]}]}}},
				{"$group": {"_id": "None", "ptids": {"$addToSet": "$_id"}}},
			]

			docs = self.tel_db[data_collection_name].aggregate(ap_stmt, allowDiskUse=True)
			i = 0
			for doc in docs:
				i += 1
				result += doc["ptids"]
		return result
	

	def efcfcd_diamond_v4_1(self, event1_list, event2_list, delta_max, delta_max_op="lt",cooccurrence=True,negation=False):
		candidates = self.get_candidates(event1_list, event2_list)
		# number of candidates is sum of len of values
		n_candidates = sum([len(x) for x in candidates.values()])
		print(f"candidates: {n_candidates}")
		result = []
		delta_min_op = "gt"
		if cooccurrence:
			delta_min_op = "gte"

		if negation:
			match_stmt = {
            "$match": {
                "e2_fc": {
                    "$not": {
                        "$elemMatch": {"$"+delta_max_op: delta_max}
                    }
                }
            }
        }
		else:
			match_stmt = {
            "$match": {
                "e2_fc": {
                    "$elemMatch": {"$"+delta_min_op: 0, "$"+delta_max_op: delta_max}
                }
            }
        }

		for pt_group_id,ptid_list in candidates.items():
			ap_stmt = [
				# find pt_group = pt_group, event1 = event1_list or event2 = event2_list
				{"$match": {
					"$or": [
						{"ptid": {"$in": ptid_list}, "event1": {"$in": event1_list}},
						{"ptid": {"$in": ptid_list}, "event2": {"$in": event2_list}}
					]
				}},
				{"$group": {"_id": "$ptid", "e1_i": {"$push": {"$cond": [{"$ne": ["$event1", None]}, "$indices", []]}}, "e2_fc": {"$push": {"$cond": [{"$ne": ["$event2", None]}, "$fc_date_diffs", None]}}}},
				# Filter out null values from arrays
				{"$project": {
					"e1_i": {
						"$reduce": {
							"input": "$e1_i",
							"initialValue": [],
							"in": {"$concatArrays": ["$$value", "$$this"]}
						}
					},  
					"e2_fc": {
							"$filter": {
									"input": "$e2_fc",
									"as": "item",
									"cond": {"$ne": ["$$item", None]}
							}
					},
				}},
				{"$match": {"e1_i": {"$ne": []}, "e2_fc": {"$ne": []}}},
				{"$project": {
					"_id": 1,
					"e2_fc": {
						"$map":{
							"input": "$e1_i",
							"as": "index",
							"in": {
								# get min element at (index + shift) for each element in e2_fc
								"$min": {
									"$map": {
										"input": "$e2_fc",
										"as": "fc",
										"in": {"$arrayElemAt": ["$$fc", "$$index"]}
									}
								}

							}
						}
					}}
				},
				match_stmt,
				{"$group": {"_id": None, "ptids": {"$addToSet": "$_id"}}},
			]
			docs = self.tel_db["fcs"].aggregate(ap_stmt, allowDiskUse=False)
			i = 0
	
			# for doc in docs:
			# 	i += 1
			# 	if doc["_id"] == "s6748":
			# 		print(doc)
			# 		break
			# doc["abc"]
			for doc in docs:
				i += 1
				result += doc["ptids"]
		return result

	def get_candidates(self, event1_list, event2_list):
		# get candidates from eii
		ap_stmt = [
			{"$match": {"pt_group":{"$gte":0}, "event": {"$in": event1_list+event2_list}}},
			{"$unwind": "$ptids"},
			{"$group": {"_id": "$ptids", 
							 "pt_group": {"$first": "$pt_group"},
							 "e1": {"$sum": {"$cond": [{"$in": ["$event", event1_list]}, 1, 0]}}, 
							 "e2": {"$sum": {"$cond": [{"$in": ["$event", event2_list]}, 1, 0]}}}
							 },
			{"$match": {"e1": {"$gt": 0}, "e2": {"$gt": 0}}},
			{"$group": {"_id": "$pt_group", "ptids": {"$addToSet": "$_id"}}}
		]
		docs = self.tel_db["eii"].aggregate(ap_stmt, allowDiskUse=True)
		candidates = {}
		for doc in docs:
			candidates[doc["_id"]] = doc["ptids"]
		return candidates

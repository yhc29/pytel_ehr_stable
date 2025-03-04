import pymongo
import os
import time

class OMOP:
  def __init__(self, mongo_url, db_name):
    self.client = pymongo.MongoClient(mongo_url)
    self.omop_db = self.client[db_name]

  def import_omop(self, omop_folder):
    # check if the folder exists
    try:
      os.listdir(omop_folder)
    except:
      print(f"Folder {omop_folder} does not exist")
      return

    # files = ["CONCEPT.csv", "CONCEPT_ANCESTOR.csv", "CONCEPT_RELATIONSHIP.csv", "CONCEPT_SYNONYM.csv", "CONCEPT_CLASS.csv", "DOMAIN.csv", "DRUG_STRENGTH.csv", "RELATIONSHIP.csv", "VOCABULARY.csv"]
    files = ["CONCEPT_CPT4.csv"]
    for file in files:
      # check if the folder contains the required files, all file names are in upper case
      if file not in os.listdir(omop_folder):
        print("Missing file: " + file)
        return
      collection_name = file.split(".")[0].lower()
      if collection_name == "concept_cpt4":
        collection_name = "concept"
      self.import_omop_file(os.path.join(omop_folder, file), collection_name,drop=False)

  
  def import_omop_file(self, file, collection_name,drop=True):
    start_time = time.time()
    # drop the collection
    if drop:
      self.omop_db[collection_name].drop()
      print(f"Dropped collection {collection_name}")

    print(f"Importing file {file} into collection {collection_name}")
    try:
      # open csv file with headers, split by tab
      import csv
      with open(file, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter='\t')
        headers = next(csv_reader)
        # import the data into the collection by batch
        batch_size = 5000
        batch_docs = []
        doc_count = 0
        collection = self.omop_db[collection_name]
        for line in csv_reader:
          record = {}
          for i in range(len(headers)):
            record[headers[i]] = line[i]
          batch_docs.append(record)
          doc_count += 1
          if len(batch_docs) >= batch_size:
            collection.insert_many(batch_docs)
            batch_docs = []
        if len(batch_docs) > 0:
          collection.insert_many(batch_docs)
        print(f"Imported {doc_count} docs into collection {collection_name}")

    except:
      print(f"Error reading file {file}")
      return False
    end_time = time.time()
    # print running time in hours:minutes:seconds
    print(f"Running time: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")

    return True
  
  def build_indices(self):
    # concept collection
    self.omop_db["concept"].create_index([("concept_id", pymongo.ASCENDING)])
    self.omop_db["concept"].create_index([("concept_name", pymongo.ASCENDING)])

  def get_concept_by_id(self, concept_id):
    doc = self.omop_db["concept"].find_one({"concept_id": concept_id})
    return doc
  
  def concept_name_search(self, concept_name, vocabulary_id=None, fuzzy=False):
    if fuzzy:
      query = {"concept_name": {"$regex": concept_name, "$options": "i"}}
    else:
      query = {"concept_name": concept_name}
    if vocabulary_id:
      query["vocabulary_id"] = vocabulary_id
    docs = self.omop_db["concept"].find(query)

    return docs
  
  def concept_code_search(self, concept_code, vocabulary_id=None, fuzzy=False):
    if fuzzy:
      query = {"concept_code": {"$regex": concept_code, "$options": "i"}}
    else:
      query = {"concept_code": concept_code}
    if vocabulary_id:
      query["vocabulary_id"] = vocabulary_id
    docs = self.omop_db["concept"].find(query)

    return docs

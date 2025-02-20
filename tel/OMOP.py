import pymongo
import os

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

    files = ["CONCEPT.CSV", "CONCEPT_ANCESTOR.CSV", "CONCEPT_RELATIONSHIP.CSV", "CONCEPT_SYNONYM.CSV", "CONCEPT_CLASS.CSV", "DOMAIN.CSV", "DRUG_STRENGTH.CSV", "RELATIONSHIP.CSV", "VOCABULARY.CSV"]
    for file in files:
      # check if the folder contains the required files, all file names are in upper case
      if file not in os.listdir(omop_folder):
        print("Missing file: " + file)
        return
      collection_name = file.split(".")[0].lower()
      self.import_omop_file(os.path.join(omop_folder, file), collection_name)

  
  def import_omop_file(self, file, collection_name):
    try:
      # open file and get headers
      with open(file, "r") as f:
        headers = f.readline().strip().split(",")
        # read the rest of the file
        data = f.readlines()
    except:
      print(f"Error reading file {file}")
      return False
    # drop the collection
    self.omop_db[collection_name].drop()
    # import the data into the collection by batch
    batch_size = 5000
    batch_docs = []
    doc_count = 0
    collection = self.omop_db[collection_name]
    for line in data:
      record = dict(zip(headers, line.strip().split(",")))
      batch_docs.append(record)
      doc_count += 1
      if len(batch_docs) >= batch_size:
        collection.insert_many(batch_docs)
        batch_docs = []
    if len(batch_docs) > 0:
      collection.insert_many(batch_docs)
    print(f"Imported {doc_count} docs into collection {collection_name}")
    return True
      

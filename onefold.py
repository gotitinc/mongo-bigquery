#!/usr/bin/env python

#
# Author: Jorge Chang 
#
# See license in LICENSE file.
#
# This is the main program used to ETL mongodb collections into Hive tables.
#

from pymongo import MongoClient
import argparse
import os
import glob
from bson.json_util import dumps
import codecs
import pprint
import json
from onefold_util import execute
from dw_util import Hive, GBigQuery
from cs_util import HDFSStorage, GCloudStorage


NUM_RECORDS_PER_PART = 100000
TMP_PATH = '/tmp/onefold_mongo'
CLOUD_STORAGE_PATH = 'onefold_mongo'
HADOOP_MAPREDUCE_STREAMING_LIB = "/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar"
ONEFOLD_MAPREDUCE_JAR = os.getcwd() + "/java/MapReduce/target/MapReduce-0.0.1-SNAPSHOT.jar"
ONEFOLD_HIVESERDES_JAR = os.getcwd() + "/java/HiveSerdes/target/hive-serdes-1.0-SNAPSHOT.jar"

# default mapreduce params
mapreduce_params = {}
mapreduce_params["mapred.reduce.max.attempts"] = "0"
mapreduce_params["mapred.map.max.attempts"] = "0"
mapreduce_params["mapred.task.timeout"] = "12000000"
MAPREDUCE_PARAMS_STR = ' '.join(["-D %s=%s"%(k,v) for k,v in mapreduce_params.iteritems()])


# helper function to split "[datatype]-[mode]" into datatype and mode
def parse_datatype_mode (datatype_mode):
  a = datatype_mode.split("-")
  if len(a) >= 2:
    return (a[0], a[1])
  else:
    raise ValueError('Invalid datatype / mode tuple %s' % datatype_mode)

# helper function to check if "address.zip_code" is in data by spliting the jsonpath by "."
def jsonpath_get(mydict, path):
  elem = mydict
  try:
    for x in path.split("."):
      elem = elem.get(x)
  except:
    pass

  return elem


class Loader:

  # control params
  infra_type = None
  mongo_uri = None
  db_name = None
  collection_name = None
  collection_sort_by_field = None
  extract_query = None
  tmp_path = None
  schema_db_name = None
  schema_collection_name = None
  use_mr = False

  hiveserveer_host = None
  hiveserver_port = None

  gcloud_project_id = None
  gcloud_storage_bucket_id = None

  write_disposition = None
  process_array = "child_table"
  dw_database_name = None
  dw_table_name = None

  policies = None

  # mongo client and schema collection
  mongo_client = None
  mongo_schema_collection = None

  # runtime variables
  extract_file_names = []
  reject_file_names = []
  sort_by_field_min = None
  sort_by_field_max = None
  dw_table_names = []
  dw = None
  cs = None
  num_records_extracted = 0
  num_records_rejected = 0

  # policy related variables
  required_fields = {}


  def initialize(self):

    # open mongo client
    self.mongo_client = MongoClient(self.mongo_uri)

    # open schema collection
    mongo_schema_db = self.mongo_client[self.schema_db_name]
    self.mongo_schema_collection = mongo_schema_db[self.schema_collection_name]

    # if overwrite, delete schema collection
    if self.write_disposition == 'overwrite':
      self.mongo_schema_collection.remove({})

    # create data warehouse object
    if self.infra_type == 'hadoop':
      self.dw = Hive(self.hiveserveer_host, self.hiveserver_port, ONEFOLD_HIVESERDES_JAR)
      self.cs = HDFSStorage()
    elif self.infra_type == 'gcloud':
      self.dw = GBigQuery(self.gcloud_project_id, self.gcloud_storage_bucket_id)
      self.cs = GCloudStorage(self.gcloud_project_id, self.gcloud_storage_bucket_id)

    # turn policies into better data structure for use later (required_fields)
    if self.policies != None:
      for policy in self.policies:
        if 'key' in policy:
          if 'required' in policy:
            if policy['key'] not in self.required_fields == None:
              self.required_fields[policy['key']] = {}
            self.required_fields[policy['key']] = policy

          if 'data_type' in policy:
            datatype_overwrite = policy['data_type']

            if 'mode' in policy:
              mode_overwrite = policy['mode']
            else:
              mode_overwrite = 'nullable'

            self.mongo_schema_collection.update_one(
              {"key": policy['key'].replace(".", "_"), "type": "field"},
              {"$set": {"data_type": datatype_overwrite,
                        "mode": mode_overwrite,
                        "forced": True}},
              upsert = True)


  def extract_data(self):

    # create tmp_path folder if necessary
    if not os.path.exists(os.path.join(self.tmp_path, self.collection_name, 'data')):
      os.makedirs(os.path.join(self.tmp_path, self.collection_name, 'data'))

    if not os.path.exists(os.path.join(self.tmp_path, self.collection_name, 'rejected')):
      os.makedirs(os.path.join(self.tmp_path, self.collection_name, 'rejected'))

    # delete old tmp files if exists
    for old_file in glob.glob(os.path.join(self.tmp_path, self.collection_name, 'data', '*')):
      print "Deleting old file %s" % (old_file)
      os.remove(old_file)

    for old_file in glob.glob(os.path.join(self.tmp_path, self.collection_name, 'rejected', '*')):
      print "Deleting old file %s" % (old_file)
      os.remove(old_file)

    # some state variables
    part_num = 0
    extract_file = None

    reject_part_num = 0
    reject_file = None

    # start mongo client
    db = self.mongo_client[self.db_name]
    collection = db[self.collection_name]

    # turn query string into json
    if self.extract_query is not None:
      if 'ObjectId' in self.extract_query:
        # kinda hacky.. and dangerous! This is to evaluate an expression
        # like {"_id": {$gt:ObjectId("55401a60151a4b1a4f000001")}}
        from bson.objectid import ObjectId
        extract_query_json = eval(self.extract_query)
      else:
        extract_query_json = json.loads(self.extract_query)
    else:
      extract_query_json = None

    # query collection, sort by collection_sort_by_field
    for data in collection.find(extract_query_json).sort(self.collection_sort_by_field, 1):

      # track min and max id for auditing..
      if self.sort_by_field_min == None:
        self.sort_by_field_min = data[self.collection_sort_by_field]
      self.sort_by_field_max = data[self.collection_sort_by_field]

      # open a new file if necessary
      if self.num_records_extracted % NUM_RECORDS_PER_PART == 0:

        if extract_file != None:
          extract_file.close()

        part_num += 1
        extract_file_name = os.path.join(self.tmp_path, self.collection_name, 'data', str(part_num))
        extract_file = open(extract_file_name, "w")
        extract_file_codec = codecs.getwriter("utf-8")(extract_file)
        self.extract_file_names.append(extract_file_name)
        print "Creating file %s" % extract_file_name

      # validate policies
      rejected = False
      for required_field_name, policy in self.required_fields.iteritems():
        if policy['required'] and jsonpath_get(data, required_field_name) is None:

          # --------------------------------------------------------
          # document found that doesn't contain required fields.
          # --------------------------------------------------------

          # open a new file if necessary
          if self.num_records_rejected % NUM_RECORDS_PER_PART == 0:

            if reject_file != None:
              reject_file.close()

            reject_part_num += 1
            reject_file_name = os.path.join(self.tmp_path, self.collection_name, 'rejected', str(reject_part_num))
            reject_file = open(reject_file_name, "w")
            reject_file_codec = codecs.getwriter("utf-8")(reject_file)
            self.reject_file_names.append(reject_file_name)
            print "Creating reject file %s" % reject_file_name

          self.num_records_rejected += 1
          reject_file_codec.write("Rejected. Missing %s. Data: %s" % (required_field_name, dumps(data)))
          reject_file_codec.write('\n')

          rejected = True
          break

      if not rejected:
        self.num_records_extracted += 1
        extract_file_codec.write(dumps(data))
        extract_file_codec.write('\n')

    if extract_file != None:
      extract_file.close()

    if reject_file != None:
      reject_file.close()

  def simple_schema_gen(self):
    command = "cat %s | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py %s/%s/%s > /dev/null" \
              % (' '.join(self.extract_file_names), self.mongo_uri, self.schema_db_name, self.schema_collection_name)
    execute(command)


  def mr_schema_gen(self):

    hdfs_data_folder = "%s/%s/data" % (CLOUD_STORAGE_PATH, self.collection_name)
    hdfs_mr_output_folder = "%s/%s/schema_gen/output" % (CLOUD_STORAGE_PATH, self.collection_name)

    # delete folders
    self.cs.rmdir(hdfs_data_folder)
    self.cs.rmdir(hdfs_mr_output_folder)
    

    # copy extracted files to hdfs data folder
    self.cs.mkdir(hdfs_data_folder)

    for extract_file_name in self.extract_file_names:
      self.cs.copy_from_local(extract_file_name, hdfs_data_folder)

    hadoop_command = """hadoop jar %s \
                              -D mapred.job.name="onefold-mongo-generate-schema" \
                              %s \
                              -input %s -output %s \
                              -mapper 'json/generate-schema-mapper.py' \
                              -reducer 'json/generate-schema-reducer.py %s/%s/%s' \
                              -file json/generate-schema-mapper.py \
                              -file json/generate-schema-reducer.py
    """ % (HADOOP_MAPREDUCE_STREAMING_LIB, MAPREDUCE_PARAMS_STR, hdfs_data_folder,
           hdfs_mr_output_folder, self.mongo_uri,
           self.schema_db_name, self.schema_collection_name)
    execute(hadoop_command)


  def simple_data_transform(self):

    hdfs_mr_output_folder = "%s/%s/data_transform/output" % (CLOUD_STORAGE_PATH, self.collection_name)
    transform_data_tmp_path = "%s/%s/data_transform/output" % (self.tmp_path, self.collection_name)

    command = "cat %s | json/transform-data-mapper.py %s/%s/%s,%s > /dev/null" \
              % (' '.join(self.extract_file_names), self.mongo_uri, self.schema_db_name,
                 self.schema_collection_name, transform_data_tmp_path)
    execute(command)

    # delete folders
    self.cs.rmdir (hdfs_mr_output_folder)

    # manually copy files into hdfs
    fragment_values = self.get_fragments()
    for fragment_value in fragment_values:
      self.cs.mkdir("%s/%s" % (hdfs_mr_output_folder, fragment_value))
      self.cs.copy_from_local("%s/%s/part-00000" % (transform_data_tmp_path, fragment_value),
                              "%s/%s/" % (hdfs_mr_output_folder, fragment_value))
      

  def mr_data_transform(self):

    hdfs_data_folder = "%s/%s/data" % (CLOUD_STORAGE_PATH, self.collection_name)
    hdfs_mr_output_folder = "%s/%s/data_transform/output" % (CLOUD_STORAGE_PATH, self.collection_name)

    # delete folders
    self.cs.rmdir(hdfs_mr_output_folder)

    hadoop_command = """hadoop jar %s \
                              -libjars %s \
                              -D mapred.job.name="onefold-mongo-transform-data" \
                              -D mapred.reduce.tasks=0 \
                              %s \
                              -input %s -output %s \
                              -mapper 'json/transform-data-mapper.py %s/%s/%s' \
                              -file json/transform-data-mapper.py \
                              -outputformat com.onefold.hadoop.MapReduce.TransformDataMultiOutputFormat
    """ % (HADOOP_MAPREDUCE_STREAMING_LIB, ONEFOLD_MAPREDUCE_JAR, MAPREDUCE_PARAMS_STR, hdfs_data_folder, hdfs_mr_output_folder, self.mongo_uri,
           self.schema_db_name, self.schema_collection_name)
    execute(hadoop_command)


  # retrieve schema tree from schema collection
  def retrieve_schema_fields(self):

    # read schema from mongodb schema collection
    schema_fields = []

    mongo_schema_fields = self.mongo_schema_collection.find({"type": "field"})
    for mongo_schema_field in mongo_schema_fields:
      schema_fields.append(mongo_schema_field)

    # add hash code to field
    field = {}
    field['key'] = "hash_code"
    field['mode'] = "nullable"
    field['data_type'] = "string"
    schema_fields.append(field)

    return schema_fields


  def get_fragments(self):
    fragment_record = self.mongo_schema_collection.find_one({"type": "fragments"})
    if fragment_record != None:
      return fragment_record['fragments']
    else:
      return []


  def load_table_hive (self, shard_value = None, table_name = None, different_table_per_shard = False, data_import_id = None):

    # if shard_value is None:
    #   gcs_uri = "%s/data/*" % (self.mr4_output_folder_uri)
    # else:
    #   gcs_uri = "%s/data/%s/*" % (self.mr4_output_folder_uri, shard_value)

    if different_table_per_shard:
      full_table_name = "%s_%s" % (table_name, shard_value)
    else:
      full_table_name = "%s" % (table_name)

    cloud_storage_path = "%s/%s/data_transform/output/%s/" % (CLOUD_STORAGE_PATH, self.collection_name, shard_value)
    self.dw.load_table(self.dw_database_name, full_table_name, cloud_storage_path)

    # extract bq_job_id and save to db
    return "%s/%s" % (data_import_id, shard_value)


  def load_dw (self):

    # retrieve schema fields from mongodb schema collection
    schema_fields = self.retrieve_schema_fields()

    # create tables
    if self.write_disposition == 'overwrite':
      if self.dw.table_exists(self.dw_database_name, self.dw_table_name):
        self.dw.delete_table(self.dw_database_name, self.dw_table_name)
      self.dw_table_names = self.dw.create_table(self.dw_database_name, self.dw_table_name, schema_fields, self.process_array)
    else:
      # if append, update table.
      if self.dw.table_exists(self.dw_database_name, self.dw_table_name):
        self.dw_table_names = self.dw.update_table(self.dw_database_name, self.dw_table_name, schema_fields)
      else:
        self.dw_table_names = self.dw.create_table(self.dw_database_name, self.dw_table_name, schema_fields, self.process_array)

    # load data
    fragment_values = self.get_fragments()

    if fragment_values == None or len(fragment_values) == 0:
      table_name = self.dw_table_name
      self.load_table_hive(shard_value = None, table_name = table_name, different_table_per_shard=False, data_import_id=None)

    else:
      for fragment_value in fragment_values:
        print "Loading fragment: " + fragment_value
        if fragment_value == 'root':
          table_name = self.dw_table_name
        else:
          table_name = self.dw_table_name + "_" + fragment_value

        self.load_table_hive(shard_value = fragment_value, table_name = table_name, different_table_per_shard=False, data_import_id=None)


  def run(self):
    # init (start mongo client)
    self.initialize()

    # extract data from Mongo
    self.extract_data()

    if self.num_records_extracted > 0:
      # generate schema and transform data
      if self.use_mr:
        self.mr_schema_gen()
        self.mr_data_transform()
      else:
        self.simple_schema_gen()
        self.simple_data_transform()

      # Create data warehouse tables and load data into them
      self.load_dw()

    print '-------------------'
    print '    RUN SUMMARY'
    print '-------------------'
    print 'Num records extracted %s' % self.num_records_extracted
    print 'Num records rejected %s' % self.num_records_rejected
    print 'Extracted data with %s from %s to %s' % (self.collection_sort_by_field, self.sort_by_field_min, self.sort_by_field_max)
    print 'Extracted files are located at: %s' % (' '.join(self.extract_file_names))
    print 'Destination Tables: %s' % (' '.join(self.dw_table_names))
    print 'Schema is stored in Mongo %s.%s' % (self.schema_db_name, self.schema_collection_name)

def usage():
  # ./onefold.py --mongo mongodb://173.255.115.8:27017 --source_db test --source_collection uber_events --schema_db test --schema_collection uber_events_schema --hiveserver_host 130.211.146.208 --hiveserver_port 10000
  # ./onefold.py --mongo mongodb://173.255.115.8:27017 --source_db test --source_collection uber_events --schema_db test --schema_collection uber_events_schema --hiveserver_host 130.211.146.208 --hiveserver_port 10000 --use_mr
  pass

def main():

  # parse command line
  parser = argparse.ArgumentParser(description='Generate schema for MongoDB collections.')
  parser.add_argument('--mongo', metavar='mongo', type=str, required=True, help='MongoDB connectivity')
  parser.add_argument('--source_db', metavar='source_db', type=str, required=True, help='Source MongoDB database name')
  parser.add_argument('--source_collection', metavar='source_collection', type=str, required=True,
                      help='Source MongoDB collection name')
  parser.add_argument('--source_sort_by_field', metavar='source_sort_by_field', type=str, default='_id',
                      help='Source MongoDB collection name')
  parser.add_argument('--query', metavar='query', type=str, help='Mongo Query for filtering')
  parser.add_argument('--tmp_path', metavar='tmp_path', type=str, help='Path to store tmp file from extraction.',
                      default=TMP_PATH)
  parser.add_argument('--schema_db', metavar='schema_db', type=str,
                      help='MongoDB database name to store schema. If not provided, default to source db.')
  parser.add_argument('--schema_collection', metavar='schema_collection', type=str,
                      help='MongoDB collection name to store schema. If not provided, default to [source_collection]_schema')
  parser.add_argument('--write_disposition', metavar='write_disposition', type=str,
                      help='overwrite or append. Default is overwrite', default='overwrite', choices=['overwrite', 'append'])
  parser.add_argument('--dest_db_name', metavar='dest_db_name', type=str,
                      help='Hive database name. If not provided, default to \'default\' hive database.')
  parser.add_argument('--dest_table_name', metavar='dest_table_name', type=str,
                      help='Hive table name. If not provided, default to source collection name.')
  parser.add_argument('--use_mr', action='store_true')
  parser.add_argument('--policy_file', metavar='policy_file', type=str,
                      help='Data Policy file name.')
  parser.add_argument('--infra_type', metavar='infra_type', type=str, default='hadoop',
                      help='Infrastructure type. One of hadoop or gcloud')

  # hive related parameters
  parser.add_argument('--hiveserver_host', metavar='hiveserver_host', type=str, required=False, help='Hiveserver host')
  parser.add_argument('--hiveserver_port', metavar='hiveserver_port', type=str, required=False, help='Hiveserver port')

  # gcloud related parameters
  parser.add_argument('--gcloud_project_id', metavar='gcloud_project_id', type=str, required=False, help='GCloud project id')
  parser.add_argument('--gcloud_storage_bucket_id', metavar='gcloud_storage_bucket_id', type=str, required=False, help='GCloud storage bucket id')

  args = parser.parse_args()

  # global mongo_uri, db_name, collection_name, extract_query, tmp_path, schema_db_name, schema_collection_name, use_mr
  loader = Loader()
  loader.infra_type = args.infra_type
  loader.mongo_uri = args.mongo
  loader.db_name = args.source_db
  loader.collection_name = args.source_collection
  loader.collection_sort_by_field = args.source_sort_by_field
  loader.extract_query = args.query
  loader.tmp_path = args.tmp_path

  if args.schema_db != None:
    loader.schema_db_name = args.schema_db
  else:
    loader.schema_db_name = args.source_db

  if args.schema_collection != None:
    loader.schema_collection_name = args.schema_collection
  else:
    loader.schema_collection_name = "%s_schema" % args.source_collection

  if args.infra_type == 'hadoop':
    if args.hiveserver_host is None:
      raise ValueError("hiveserver_host must be specified for 'hadoop' infrastructure type.")
    if args.hiveserver_port is None:
      raise ValueError("hiveserver_port must be specified for 'hadoop' infrastructure type.")

    loader.hiveserver_host = args.hiveserver_host
    loader.hiveserver_port = args.hiveserver_port
  else:
    if args.gcloud_project_id is None:
      raise ValueError("gcloud_project_id must be specified for 'gcloud' infrastructure type.")
    if args.gcloud_storage_bucket_id is None:
      raise ValueError("gcloud_storage_bucket_id must be specified for 'gcloud' infrastructure type.")

    loader.gcloud_project_id = args.gcloud_project_id
    loader.gcloud_storage_bucket_id = args.gcloud_storage_bucket_id

  loader.write_disposition = args.write_disposition

  if args.dest_table_name != None:
    loader.dw_table_name = args.dest_table_name
  else:
    loader.dw_table_name = args.source_collection

  if args.dest_db_name != None:
    loader.dw_database_name = args.dest_db_name

  if args.use_mr:
    loader.use_mr = args.use_mr

  if args.policy_file != None:
    # open policy file
    policy_file = open(args.policy_file, "r")
    loader.policies = json.loads(policy_file.read())

  loader.run()


if __name__ == '__main__':
  main()

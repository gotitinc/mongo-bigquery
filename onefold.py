#!/usr/bin/env python

from pymongo import MongoClient
import argparse
import os
import glob
from bson.json_util import dumps
import codecs
import pprint
import json
from onefold_util import execute
from dw_util import Hive


NUM_RECORDS_PER_PART = 5
TMP_PATH = '/tmp/onefold_mongo'
HDFS_PATH = 'onefold_mongo'
HADOOP_MAPREDUCE_STREAMING_LIB = "/usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar"
HIVE_SERDES_LIB = os.getcwd() + "/hive-serdes-1.0-SNAPSHOT.jar"

# default mapreduce params
mapreduce_params = {}
mapreduce_params["mapred.reduce.max.attempts"] = "0"
mapreduce_params["mapred.map.max.attempts"] = "0"
mapreduce_params["mapred.task.timeout"] = "12000000"
MAPREDUCE_PARAMS_STR = ' '.join(["-D %s=%s"%(k,v) for k,v in mapreduce_params.iteritems()])


class Loader:

  # control params
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

  write_disposition = None
  process_array = "child_table"
  dw_database_name = None
  dw_table_name = None

  # mongo client and schema collection
  mongo_client = None
  mongo_schema_collection = None

  # runtime variables
  extract_file_names = []
  sort_by_field_min = None
  sort_by_field_max = None
  dw_table_names = []
  dw = None


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
    self.dw = Hive(self.hiveserveer_host, self.hiveserver_port, HIVE_SERDES_LIB)


  def extract_data(self):

    # create tmp_path folder if necessary
    if not os.path.exists(os.path.join(self.tmp_path, self.collection_name, 'data')):
      os.makedirs(os.path.join(self.tmp_path, self.collection_name, 'data'))

    # delete old tmp files if exists
    for old_file in glob.glob(os.path.join(self.tmp_path, self.collection_name, 'data', '*')):
      print "Deleting old file %s" % (old_file)
      os.remove(old_file)

    # some state variables
    part_num = 0
    extract_file = None

    # start mongo client
    db = self.mongo_client[self.db_name]
    collection = db[self.collection_name]

    # iterate through the collection
    index = 0

    # turn query string into json
    if 'ObjectId' in self.extract_query:
      # kinda hacky.. and dangerous! This is to evaluate an expression
      # like {"_id": {$gt:ObjectId("55401a60151a4b1a4f000001")}}
      from bson.objectid import ObjectId
      extract_query_json = eval(self.extract_query)
    else:
      extract_query_json = json.loads(self.extract_query)

    # query collection, sort by collection_sort_by_field
    for data in collection.find(extract_query_json).sort(self.collection_sort_by_field, 1):

      # track min and max id for auditing..
      if self.sort_by_field_min == None:
        self.sort_by_field_min = data[self.collection_sort_by_field]
      self.sort_by_field_max = data[self.collection_sort_by_field]

      # open a new file if necessary
      if index % NUM_RECORDS_PER_PART == 0:

        if extract_file != None:
          extract_file.close()

        part_num += 1
        extract_file_name = os.path.join(self.tmp_path, self.collection_name, 'data', str(part_num))
        extract_file = open(extract_file_name, "w")
        extract_file_codec = codecs.getwriter("utf-8")(extract_file)
        self.extract_file_names.append(extract_file_name)
        print "Creating file %s" % extract_file_name

      index += 1
      extract_file_codec.write(dumps(data))
      extract_file_codec.write('\n')

    if extract_file != None:
      extract_file.close()


  def simple_schema_gen(self):
    command = "cat %s | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py %s/%s/%s > /dev/null" \
              % (' '.join(self.extract_file_names), self.mongo_uri, self.schema_db_name, self.schema_collection_name)
    execute(command)


  def mr_schema_gen(self):

    hdfs_data_folder = "%s/%s/data" % (HDFS_PATH, self.collection_name)
    hdfs_mr_output_folder = "%s/%s/schema_gen/output" % (HDFS_PATH, self.collection_name)

    # delete folders
    execute("hadoop fs -rm -r -f %s" % hdfs_data_folder)
    execute("hadoop fs -rm -r -f %s" % hdfs_mr_output_folder)

    # copy extracted files to hdfs data folder
    execute("hadoop fs -mkdir -p %s" % hdfs_data_folder)
    for extract_file_name in self.extract_file_names:
      execute("hadoop fs -copyFromLocal %s %s/" % (extract_file_name, hdfs_data_folder))

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

    hdfs_mr_output_folder = "%s/%s/data_transform/output" % (HDFS_PATH, self.collection_name)
    transform_data_tmp_path = "%s/%s/data_transform/output" % (self.tmp_path, self.collection_name)

    command = "cat %s | json/transform-data-mapper.py %s/%s/%s,%s > /dev/null" \
              % (' '.join(self.extract_file_names), self.mongo_uri, self.schema_db_name,
                 self.schema_collection_name, transform_data_tmp_path)
    execute(command)

    # delete folders
    execute("hadoop fs -rm -r -f %s" % hdfs_mr_output_folder)

    # manually copy files into hdfs
    fragment_values = self.get_fragments()
    for fragment_value in fragment_values:
      execute("hadoop fs -mkdir -p %s/%s" % (hdfs_mr_output_folder, fragment_value), ignore_error=True)
      execute("hadoop fs -copyFromLocal %s/%s/part-00000 %s/%s/" % (transform_data_tmp_path, fragment_value,
                                                                    hdfs_mr_output_folder, fragment_value))


  def mr_data_transform(self):

    hdfs_data_folder = "%s/%s/data" % (HDFS_PATH, self.collection_name)
    hdfs_mr_output_folder = "%s/%s/data_transform/output" % (HDFS_PATH, self.collection_name)

    # delete folders
    execute("hadoop fs -rm -r -f %s" % hdfs_mr_output_folder)


    hadoop_command = """hadoop jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming.jar \
                              -libjars java/MapReduce/target/MapReduce-0.0.1-SNAPSHOT.jar \
                              -D mapred.job.name="onefold-mongo-transform-data" \
                              -D mapred.reduce.tasks=0 \
                              %s \
                              -input %s -output %s \
                              -mapper 'json/transform-data-mapper.py %s/%s/%s' \
                              -file json/transform-data-mapper.py \
                              -outputformat com.onefold.hadoop.MapReduce.TransformDataMultiOutputFormat
    """ % (MAPREDUCE_PARAMS_STR, hdfs_data_folder, hdfs_mr_output_folder, self.mongo_uri,
           self.schema_db_name, self.schema_collection_name)
    execute(hadoop_command)


  # create table
  def generate_table_schema(self):

    # read schema from mongodb schema collection
    schema = []

    mongo_schema_fields = self.mongo_schema_collection.find({"type": "field"})
    schema_fields = {}
    for mongo_schema_field in mongo_schema_fields:
      schema_fields[mongo_schema_field['key']] = mongo_schema_field['data_type']

    # sort schema fields
    sorted_schema_fields = sorted(schema_fields.iteritems())

    for name, data_type_mode in sorted_schema_fields:

      a = data_type_mode.split('-')
      if len(a) >= 2:
        data_type = a[0]
        mode = a[1]
      else:
        data_type = a[0]
        mode = None

      if '.' in name:

        chunks = name.split(".")

        parent_names = chunks[0:-1]
        child_name = chunks[-1]

        field = {}
        field['name'] = child_name
        field['mode'] = mode
        field['type'] = data_type

        parent_field = schema

        # drill down to find the right parent_field
        for parent_name in parent_names:
          if type(parent_field) is dict:
            parent_field = [item for item in parent_field['fields'] if item['name'] == parent_name][0]
          elif type(parent_field) is list:
            parent_field = [item for item in parent_field if item['name'] == parent_name][0]

          if parent_field is not None:
            if 'fields' not in parent_field:
              parent_field['fields'] = []

        parent_field['fields'].append(field)

      else:
        field = {}
        field['name'] = name
        field['mode'] = mode
        field['type'] = data_type

        schema.append(field)

    field = {}
    field['name'] = "hash_code"
    field['mode'] = "nullable"
    field['type'] = "string"
    schema.append(field)

    pprint.pprint(schema)
    return schema


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

    hdfs_path = "%s/%s/data_transform/output/%s/" % (HDFS_PATH, self.collection_name, shard_value)
    self.dw.load_table(self.dw_database_name, full_table_name, hdfs_path)

    # extract bq_job_id and save to db
    return "%s/%s" % (data_import_id, shard_value)


  def load_dw (self):

    table_schema = self.generate_table_schema()
    schema_json = json.dumps(table_schema)

    schema_file_name = "%s_schema.json" % (self.collection_name)
    schema_file = open(schema_file_name, "w")
    schema_file.write(schema_json)
    schema_file.close()

    # create tables
    if self.write_disposition == 'overwrite':
      if self.dw.table_exists(self.dw_database_name, self.dw_table_name):
        self.dw.delete_table(self.dw_database_name, self.dw_table_name)
      self.dw_table_names = self.dw.create_table(self.dw_database_name, self.dw_table_name, schema_file_name, self.process_array)
    else:
      # if append, update table.
      if self.dw.table_exists(self.dw_database_name, self.dw_table_name):
        self.dw_table_names = self.dw.update_table(self.dw_database_name, self.dw_table_name, schema_file_name)
      else:
        self.dw_table_names = self.dw.create_table(self.dw_database_name, self.dw_table_name, schema_file_name, self.process_array)

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

    # generate schema
    if self.use_mr:
      self.mr_schema_gen()
      self.mr_data_transform()
    else:
      self.simple_schema_gen()
      self.simple_data_transform()

    self.load_dw()

    print '-------------------'
    print '    RUN SUMMARY'
    print '-------------------'
    print 'Extracted data with %s from %s to %s' % (self.collection_sort_by_field, self.sort_by_field_min, self.sort_by_field_max)
    print 'Extracted files are located at: %s' % (' '.join(self.extract_file_names))
    print 'Hive Tables: %s' % (' '.join(self.dw_table_names))
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
  parser.add_argument('--hiveserver_host', metavar='hiveserver_host', type=str, required=True, help='Hiveserver host')
  parser.add_argument('--hiveserver_port', metavar='hiveserver_port', type=str, required=True, help='Hiveserver port')
  parser.add_argument('--hive_db_name', metavar='hive_db_name', type=str,
                      help='Hive database name. If not provided, default to \'default\' hive database.')
  parser.add_argument('--hive_table_name', metavar='hive_table_name', type=str,
                      help='Hive table name. If not provided, default to source collection name.')
  parser.add_argument('--use_mr', action='store_true')
  args = parser.parse_args()

  # global mongo_uri, db_name, collection_name, extract_query, tmp_path, schema_db_name, schema_collection_name, use_mr
  loader = Loader()
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

  loader.hiveserver_host = args.hiveserver_host
  loader.hiveserver_port = args.hiveserver_port

  loader.write_disposition = args.write_disposition

  if args.hive_table_name != None:
    loader.dw_table_name = args.hive_table_name
  else:
    loader.dw_table_name = args.schema_collection

  if args.hive_db_name != None:
    loader.dw_database_name = args.hive_db_name

  if args.use_mr:
    loader.use_mr = args.use_mr

  loader.run()


if __name__ == '__main__':
  main()

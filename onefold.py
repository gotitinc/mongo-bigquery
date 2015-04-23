#!/usr/bin/env python

from pymongo import MongoClient
import argparse
import os
import glob
from bson.json_util import dumps
import codecs
import subprocess
import time
import random

NUM_RECORDS_PER_PART = 5
HDFS_PATH = '/user/hadoop/onefold_mongo'

# global params
mongo_uri = None
db_name = None
collection_name = None
extract_query = None
tmp_path = None
schema_db_name = None
schema_collection_name = None
use_mr = False

# global variables
extract_file_names = []

# default mapreduce params
mapreduce_params = {}
mapreduce_params["mapred.reduce.max.attempts"] = "0"
mapreduce_params["mapred.map.max.attempts"] = "0"
mapreduce_params["mapred.task.timeout"] = "12000000"
mapreduce_params["mapred.tasktracker.map.tasks.maximum"] = "2"
mapreduce_params["mapred.tasktracker.reduce.tasks.maximum"] = "2"
mapreduce_params["mapred.map.tasks"] = "10"
mapreduce_params["mapred.reduce.tasks"] = "10"
mapreduce_params_str = ' '.join(["-D %s=%s"%(k,v) for k,v in mapreduce_params.iteritems()])

def execute(command, ignore_error=False, retry=False, subpress_output=False):

  if retry:
    num_retries = 5
  else:
    num_retries = 1

  l = range(0,num_retries)
  for n in l:
    try:
      print 'Executing command: %s' % command

      if subpress_output:
        devnull = open(os.devnull, 'w')
        rc = subprocess.call(command, shell=True, stdout=devnull, stderr=devnull)
      else:
        rc = subprocess.call(command, shell=True)

      if rc:
        # Non-zero return code indicates an error.
        if not ignore_error:
          raise Exception("Error executing command: %s" % command)

      # if command ran successfully, return!
      return
    except:
      if retry:
        # Apply exponential backoff.
        print 'Retry-able. Sleeping...'
        time.sleep((2 ** n) + random.randint(0, 1000) / 1000)
      else:
        raise

  # only reach this point if we've re-tried and still failed.
  if retry:
    print "Retries exceeded (%s times). Throwing exception.." % num_retries
    raise Exception ("Retries exceeded (%s times) when executing this command." % num_retries)

def extract_data():

  # create tmp_path folder if necessary
  if not os.path.exists(os.path.join(tmp_path, collection_name, 'data')):
    os.makedirs(os.path.join(tmp_path, collection_name, 'data'))

  # delete old tmp files if exists
  for old_file in glob.glob(os.path.join(tmp_path, collection_name, 'data', '*')):
    print "Deleting old file %s" % (old_file)
    os.remove(old_file)

  # some state variables
  part_num = 0
  extract_file = None

  # start mongo client
  client = MongoClient(mongo_uri)
  db = client[db_name]
  collection = db[collection_name]

  # iterate through the collection
  index = 0
  for data in collection.find(extract_query):

    # open a new file if necessary
    if index % NUM_RECORDS_PER_PART == 0:

      if extract_file != None:
        extract_file.close()

      part_num += 1
      extract_file_name = os.path.join(tmp_path, collection_name, 'data', str(part_num))
      extract_file = open(extract_file_name, "w")
      extract_file_codec = codecs.getwriter("utf-8")(extract_file)
      extract_file_names.append(extract_file_name)
      print "Creating file %s" % extract_file_name

    index += 1
    extract_file_codec.write(dumps(data))
    extract_file_codec.write('\n')

  extract_file.close()
  return extract_file_names


def simple_schema_gen():
  command = "cat %s | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py %s/%s/%s > /dev/null" \
            % (' '.join(extract_file_names), mongo_uri, schema_db_name, schema_collection_name)
  execute(command)


def mr_schema_gen():

  hdfs_data_folder = "%s/%s/data" % (HDFS_PATH, collection_name)
  hdfs_mr_output_folder = "%s/%s/schema_gen/output" % (HDFS_PATH, collection_name)

  # delete folders
  execute("hadoop fs -rm -r -f %s" % hdfs_data_folder)
  execute("hadoop fs -rm -r -f %s" % hdfs_mr_output_folder)

  # copy extracted files to hdfs data folder
  execute("hadoop fs -mkdir -p %s" % hdfs_data_folder)
  for extract_file_name in extract_file_names:
    execute("hadoop fs -copyFromLocal %s %s/" % (extract_file_name, hdfs_data_folder))

  hadoop_command = """hadoop jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming.jar \
                            -D mapred.job.name="onefold-mongo-generate-schema" \
                            %s \
                            -input %s -output %s \
                            -mapper 'json/generate-schema-mapper.py' \
                            -reducer 'json/generate-schema-reducer.py %s/%s/%s' \
                            -file json/generate-schema-mapper.py \
                            -file json/generate-schema-reducer.py
  """ % (mapreduce_params_str, hdfs_data_folder, hdfs_mr_output_folder, mongo_uri, schema_db_name, schema_collection_name)
  execute(hadoop_command)


def simple_data_transform():

  transform_data_tmp_path = "%s/%s/output" % (tmp_path, collection_name)

  command = "cat %s | json/transform-data-mapper.py %s/%s/%s,%s > /dev/null" \
            % (' '.join(extract_file_names), mongo_uri, schema_db_name,
               schema_collection_name, transform_data_tmp_path)
  execute(command)


def mr_data_transform():

  hdfs_data_folder = "%s/%s/data" % (HDFS_PATH, collection_name)
  hdfs_mr_output_folder = "%s/%s/data_transform/output" % (HDFS_PATH, collection_name)
  # transform_data_tmp_path = "%s/%s/output" % (tmp_path, collection_name)

  # delete folders
  execute("hadoop fs -rm -r -f %s" % hdfs_mr_output_folder)


  hadoop_command = """hadoop jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming.jar \
                            -libjars java/MapReduce/target/MapReduce-0.0.1-SNAPSHOT.jar \
                            -D mapred.job.name="onefold-mongo-transform-data" \
                            %s \
                            -input %s -output %s \
                            -mapper 'json/transform-data-mapper.py %s/%s/%s' \
                            -reducer 'cat' \
                            -file json/transform-data-mapper.py \
                            -outputformat com.onefold.hadoop.MapReduce.TransformDataMultiOutputFormat
  """ % (mapreduce_params_str, hdfs_data_folder, hdfs_mr_output_folder, mongo_uri, schema_db_name, schema_collection_name)
  execute(hadoop_command)


def load_hive():
  if use_mr:
    mr_data_transform()
  else:
    simple_data_transform()


def usage():
  # ./onefold.py schema_gen --mongo mongodb://173.255.115.8:27017 --source_db test --source_collection uber_events --schema_db test --schema_collection uber_events_schema
  # ./onefold.py schema_gen --mongo mongodb://173.255.115.8:27017 --source_db test --source_collection uber_events --schema_db test --schema_collection uber_events_schema --use_mr
  pass

def main():

  # parse command line
  parser = argparse.ArgumentParser(description='Generate schema for MongoDB collections.')
  parser.add_argument('--mongo', metavar='mongo', type=str, help='MongoDB connectivity')
  parser.add_argument('--source_db', metavar='source_db', type=str, help='Source MongoDB database name')
  parser.add_argument('--source_collection', metavar='source_collection', type=str, help='Source MongoDB collection name')
  parser.add_argument('--query', metavar='query', type=str, help='[Optional] Query for filtering, etc.')
  parser.add_argument('--tmp_path', metavar='tmp_path', type=str, help='Path to store tmp file from extraction.', default='/tmp/onefold_mongo')
  parser.add_argument('--schema_db', metavar='schema_db', type=str, help='MongoDB database name to store schema')
  parser.add_argument('--schema_collection', metavar='schema_collection', type=str, help='MongoDB collection name to store schema')
  parser.add_argument('--use_mr', action='store_true')
  args = parser.parse_args()

  global mongo_uri, db_name, collection_name, extract_query, tmp_path, schema_db_name, schema_collection_name, use_mr
  mongo_uri = args.mongo
  db_name = args.source_db
  collection_name = args.source_collection
  extract_query = args.query
  tmp_path = args.tmp_path
  schema_db_name = args.schema_db
  schema_collection_name = args.schema_collection
  if args.use_mr:
    use_mr = args.use_mr

  # extract data from Mongo
  extract_data()

  # generate schema
  if use_mr:
    mr_schema_gen()
  else:
    simple_schema_gen()

  load_hive()

if __name__ == '__main__':
  main()

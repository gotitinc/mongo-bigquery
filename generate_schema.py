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

NUM_RECORDS_PER_PART = 500


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

def extract_data(mongo_uri, db_name, collection_name, query, tmp_path):

  # create tmp_path folder if necessary
  if not os.path.exists(tmp_path):
    os.makedirs(tmp_path)

  # delete old tmp files if exists
  for old_file in glob.glob(os.path.join(tmp_path, collection_name + '*')):
    print "Deleting old file %s" % (old_file)
    os.remove(old_file)

  # some state variables
  part_num = 0
  extract_file = None
  extract_file_names = []

  # start mongo client
  client = MongoClient(mongo_uri)
  db = client[db_name]
  collection = db[collection_name]

  # iterate through the collection
  index = 0
  for data in collection.find(query):

    # open a new file if necessary
    if index % NUM_RECORDS_PER_PART == 0:

      if extract_file != None:
        extract_file.close()

      part_num += 1
      extract_file_name = os.path.join(tmp_path, collection_name + str(part_num))
      extract_file = open(extract_file_name, "w")
      extract_file_codec = codecs.getwriter("utf-8")(extract_file)
      extract_file_names.append(extract_file_name)
      print "Creating file %s" % extract_file_name

    index += 1
    extract_file_codec.write(dumps(data))
    extract_file_codec.write('\n')

  extract_file.close()
  return extract_file_names


def simple_schema_gen(extract_file_names, mongo_uri, schema_db_name, schema_collection_name):
  command = "cat %s | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py %s/%s/%s" \
            % (' '.join(extract_file_names), mongo_uri, schema_db_name, schema_collection_name)
  execute(command)


def mr_schema_gen(extract_file_names, mongo_uri, schema_db_name, schema_collection_name, tmp_path):

  execute("hadoop fs -rm -r onefold_mongo")
  execute("hadoop fs -rm -r onefold_mongo_output")

  execute("hadoop fs -mkdir onefold_mongo")
  for extract_file_name in extract_file_names:
    execute("hadoop fs -copyFromLocal %s/%s onefold_mongo/" % (tmp_path, extract_file_name))

  hadoop_command = """hadoop jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-streaming.jar \
                            -D mapred.reduce.max.attempts=0 \
                            -D mapred.map.max.attempts=0 \
                            -D mapred.job.name="onefold-mongo-generate-schema" \
                            -input onefold_mongo -output onefold_mongo_output \
                            -mapper 'json/generate-schema-mapper.py' \
                            -reducer 'json/generate-schema-reducer.py %s/%s/%s' \
                            -file json/generate-schema-mapper.py \
                            -file json/generate-schema-reducer.py
  """ % (mongo_uri, schema_db_name, schema_collection_name)
  execute(hadoop_command)


def usage():
  # ./generate_schema.py --mongo mongodb://173.255.115.8:27017 --source_db test --source_collection uber_events --schema_db test --schema_collection uber_events_schema
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

  # extract data from Mongo
  extract_file_names = extract_data(args.mongo, args.source_db, args.source_collection, args.query, args.tmp_path)

  # generate schema
  if args.use_mr:
    mr_schema_gen(extract_file_names, args.mongo, args.schema_db, args.schema_collection, args.tmp_path)
  else:
    simple_schema_gen(extract_file_names, args.mongo, args.schema_db, args.schema_collection)

if __name__ == '__main__':
  main()

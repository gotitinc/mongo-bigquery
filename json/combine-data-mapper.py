#!/usr/bin/env python

import re
import sys
import json
import os
import socket
import subprocess
import codecs
import hashlib
import pprint
from pymongo import MongoClient

# create utf reader and writer for stdin and stdout
output_stream = codecs.getwriter("utf-8")(sys.stdout)
input_stream = codecs.getreader("utf-8")(sys.stdin, errors="ignore")
error_stream = codecs.getwriter("utf-8")(sys.stderr)

process_array = "child_table"
shard_key = None

BATCH_SIZE = 10000
BATCH_NUM_LINES = 50000

# e.g. schema['event'] = 'string-nullable'
mongo_schema_collection = None
schema = {}
shard_values = []

# params
tmp_path = None

# create file descriptors
file_descriptors = {}

def clean_data(line, line_num, parent = None, parent_hash_code = None, is_array = False):
  new_data = {}
  new_data_fragments = {}

  # read each line into a line_hash
  try:
    data = json.loads(line, encoding="utf-8")
  except ValueError:
    print >> error_stream, "Line %i: JSON Parse Error. Data: %s" % (line_num, line)
    return None

  # create hash code
  hash_code = hashlib.sha1(json.dumps(data, sort_keys=True)).hexdigest()
  new_data['hash_code'] = hash_code

  if parent_hash_code != None:
    new_data['parent_hash_code'] = parent_hash_code

  # determine shard key (only for root level).
  if parent == None:
    if shard_key is not None:
      shard_value = get_shard_value(data, shard_key)

      if shard_value is None:
        print >> error_stream, "Line %i: Invalid shard value. Data: %s" % (line_num, line)
        return

      new_data_fragments["root/%s" % shard_value] = new_data
      shard_values.append(shard_value)
    else:
      new_data_fragments['root'] = new_data

  else:
    new_data_fragments['root'] = new_data

  if data:

    for (key, value) in data.iteritems():

      k = re.sub("[^0-9a-zA-Z_]", '_', key).lower()

      # BigQuery disallows field to start with number
      if ord(k[0]) >= 48 and ord(k[0]) <= 59:
        k = "_f" + k

      if parent == None:
        full_key = k
        dict_key = full_key
      else:
        if is_array:
          full_key = parent + "." + k
          dict_key = key
        else:
          full_key = parent + "_" + k
          dict_key = full_key

      # check to see if dict is empty - BigQuery doesn't support RECORD data type with no fields
      if isinstance(value, dict) and len(value) == 0:
        continue

      # check to see if list is empty - BigQuery doesn't support REPEATED data type with no data
      if isinstance(value, list) and len(value) == 0:
        continue

      # print error if data type is not found for this key!
      if full_key not in schema:
        print >> error_stream, "Line %i: Couldn't find data type for key %s. Skipping this value. Data: %s" % (
          line_num, full_key, line)
        continue

      data_type_mode = schema[full_key].split("-")
      data_type = data_type_mode[0]
      mode = data_type_mode[1]

      data_type_forced = False
      if len(data_type_mode) >= 3:
        data_type_forced = (data_type_mode[2] == 'forced')

      if data_type == 'record':

          if mode == 'repeated':
            if not isinstance(value, list):
              print >> error_stream, "Line %i: Expect repeated record but found %s. Data: %s" % (line_num, value, line)
              return None
            else:

              if process_array == "child_table":
                if full_key not in new_data_fragments:
                  new_data_fragments[full_key] = []

                for v in value:
                  t = clean_data(json.dumps(v, ensure_ascii=False), line_num, full_key, hash_code, True)

                  for fragment, fragment_content in t.iteritems():
                    if fragment == 'root':
                      new_data_fragments[full_key].append(fragment_content)
                    else:
                      fragment_key = re.sub("[^0-9a-zA-Z_]", '_', fragment).lower()
                      new_data_fragments[fragment_key] = fragment_content

              else:
                new_data[dict_key] = json.dumps(value)

          else:
            if not isinstance(value, dict):
              print >> error_stream, "Line %i: Expect record but found %s. Data: %s" % (line_num, value, line)
              return None
            else:
              t = clean_data(json.dumps(value, ensure_ascii=False), line_num, full_key)

              for fragment, fragment_content in t.iteritems():
                if fragment == 'root':
                  fragment_content.pop("hash_code", None)
                  new_data.update(fragment_content)

                if isinstance(fragment_content, list):
                  new_data_fragments[fragment] = fragment_content

      else:

        if value:

          # check if data type mismatch
          if data_type == 'string':

            if mode == 'repeated':
              if not isinstance(value, list):
                print >> error_stream, "Line %i: Expect repeated string but found %s. Data: %s" % (
                  line_num, value, line)
                return None
              else:

                if process_array == "child_table":
                  if full_key not in new_data_fragments:
                    new_data_fragments[full_key] = []

                  for v in value:
                    cleaned_v = unicode(v)
                    t = {"value": cleaned_v, "parent_hash_code": hash_code}
                    new_data_fragments[full_key].append(t)
                else:
                  new_data[dict_key] = json.dumps(value)

            else:
              new_data[dict_key] = unicode(value)

          elif data_type == 'float':

            if mode == 'repeated':
              if not isinstance(value, list):
                print >> error_stream, "Line %i: Expect repeated string but found %s. Data: %s" % (
                  line_num, value, line)
                return None
              else:

                if process_array == "child_table":
                  if full_key not in new_data_fragments:
                    new_data_fragments[full_key] = []

                  for v in value:

                    cleaned_v = None

                    try:
                      cleaned_v = float(v)
                    except ValueError:
                      if not data_type_forced:
                        print >> error_stream, "Line %i: Couldn't convert %s to float. Data: %s" % (
                          line_num, str(value), line)
                        return None

                    t = {"value": cleaned_v, "parent_hash_code": hash_code}
                    new_data_fragments[full_key].append(t)
                else:
                  new_data[dict_key] = json.dumps(value)

            else:
              try:
                new_data[dict_key] = float(value)
              except ValueError:
                if data_type_forced:
                  new_data[dict_key] = None
                else:
                  print >> error_stream, "Line %i: Couldn't convert %s to float. Data: %s" % (
                    line_num, str(value), line)
                  return None

          elif data_type == 'integer':

            if mode == 'repeated':
              if not isinstance(value, list):
                print >> error_stream, "Line %i: Expect repeated string but found %s. Data: %s" % (
                  line_num, value, line)
                return None
              else:

                if process_array == "child_table":
                  if full_key not in new_data_fragments:
                    new_data_fragments[full_key] = []

                  for v in value:

                    cleaned_v = None

                    try:
                      cleaned_v = int(v)
                    except ValueError:
                      if not data_type_forced:
                        print >> error_stream, "Line %i: Couldn't convert %s to int. Data: %s" % (
                          line_num, str(value), line)
                        return None

                    t = {"value": cleaned_v, "parent_hash_code": hash_code}
                    new_data_fragments[full_key].append(t)
                else:
                  new_data[dict_key] = json.dumps(value)

            else:
              try:
                new_data[dict_key] = int(value)
              except ValueError:
                if data_type_forced:
                  new_data[dict_key] = None
                else:
                  print >> error_stream, "Line %i: Couldn't convert %s to int. Data: %s" % (line_num, str(value), line)
                  return None

          elif data_type == 'boolean':

            if mode == 'repeated':
              if not isinstance(value, list):
                print >> error_stream, "Line %i: Expect repeated string but found %s. Data: %s" % (
                  line_num, value, line)
                return None
              else:

                if process_array == "child_table":
                  if full_key not in new_data_fragments:
                    new_data_fragments[full_key] = []

                  for v in value:
                    t = {"value": str(v).lower() == 'true', "parent_hash_code": hash_code}
                    new_data_fragments[full_key].append(t)
                else:
                  new_data[dict_key] = json.dumps(value)

            else:
              new_data[dict_key] = (str(value).lower() == 'true')

          else:

            if mode == 'repeated':
              if not isinstance(value, list):
                print >> error_stream, "Line %i: Expect repeated string but found %s. Data: %s" % (
                  line_num, value, line)
                return None
              else:

                if process_array == "child_table":
                  if full_key not in new_data_fragments:
                    new_data_fragments[full_key] = []

                  for v in value:
                    cleaned_v = unicode(v)
                    t = {"value": cleaned_v, "parent_hash_code": hash_code}
                    new_data_fragments[full_key].append(t)
                else:
                  new_data[dict_key] = json.dumps(value)

            else:
              new_data[dict_key] = unicode(value)

        else:
          new_data[dict_key] = None

  return new_data_fragments

def get_shard_value(data, shard_key):

  # split shard key by "."
  tmp = data
  shard_key_parts = shard_key.split(".")
  for shard_key_part in shard_key_parts:
    if shard_key_part in tmp:
      tmp = tmp[shard_key_part]
    else:
      return None

  if isinstance(tmp, dict):
    return None
  else:

    shard_value = str(tmp)

    if len(shard_value) > 32 or len(shard_value) <= 0:
      return None

    shard_value = re.sub("[^0-9a-zA-Z_]", '_', shard_value).lower()
    return shard_value

def create_file_descriptor(fragment_value, shard_value = None):
  # generate unique local file
  local_file_name = '%s_%s' % (socket.gethostbyname(socket.gethostname()), os.getpid())

  path = fragment_value
  if shard_value != None:
    path = fragment_value + "/" + shard_value

  # creating folder and opening file
  execute('mkdir %s/%s' % (tmp_path, path), ignore_error=True)
  file_name = '%s/%s/%s' % (tmp_path, path, local_file_name)
  print >> error_stream, "Opening file descriptor %s" % file_name
  file = open(file_name, 'w')
  file_descriptors[path] = {"file": file, "file_name": file_name}
  print >> error_stream, "Opened file descriptor %s" % file_name

def process_line(line, line_num):
  # clean data
  data_fragments = clean_data(line, line_num, None)

  # skip if data is not clean..
  if data_fragments is None or len(data_fragments) == 0:
    return

  # handle other fragments
  for fragment_value, fragment_content in data_fragments.iteritems():

    # open local file descriptor for this fragment
    if fragment_value not in file_descriptors:
      create_file_descriptor(fragment_value)

    file = file_descriptors[fragment_value]["file"]

    if isinstance(fragment_content, list):
      for element in fragment_content:
        # write data to local file
        file.write(json.dumps(element))
        file.write('\n')
    else:
      # write data to local file
      file.write(json.dumps(fragment_content))
      file.write('\n')


def execute(command, ignore_error=False):
  print >> error_stream, 'Executing command: %s' % command
  if subprocess.call(command, shell=True):
    # Non-zero return code indicates an error.
    if not ignore_error:
      raise Exception("Error executing command: %s" % command)


def usage():
  print "Usage: %s mongodb://[host]:[port]/[db_name]/[schema_collection_name],[hdfs_output_path],[tmp_path]" % sys.argv[0]
  sys.exit(2)


def main(argv):

  if len(argv) < 0:
    usage()

  try:

    global tmp_path, mongo_schema_collection

    args = argv[0].split(",")
    schema_arg = args[0]
    output_path = args[1]
    tmp_path = args[2]

    schema_args = schema_arg.split("/")
    schema_collection_name = schema_args[-1]
    schema_db_name = schema_args[-2]
    mongo_uri = '/'.join(schema_args[0:-2])

    client = MongoClient(mongo_uri)
    db = client[schema_db_name]

    mongo_schema_collection = db[schema_collection_name]

  except ValueError:
    usage()

  # create tmp folder to store file
  execute('rm -rf %s' % tmp_path, ignore_error=True)
  execute('mkdir %s' % tmp_path, ignore_error=True)

  global schema, process_array, shard_key

  # read schema from mongodb server
  schema_fields = mongo_schema_collection.find({"type": "field"})
  for schema_field in schema_fields:
    schema[schema_field['key']] = schema_field['data_type']

  # read process_array from redis
  # if redis_server.hget('%s/policy' % app_id, "process_array") != None:
  #   process_array = redis_server.hget('%s/policy' % app_id, "process_array")
  #
  # # read shard_key from redis
  # if redis_server.hget('%s/policy' % app_id, "shard_key") != None:
  #   shard_key = redis_server.hget('%s/policy' % app_id, "shard_key")

  # process input
  line_num = 1
  for line in input_stream:
    process_line(line, line_num)
    line_num += 1

    # print something to stderr and stdout every 1000 lines
    if line_num % 1000 == 0:
      print >> error_stream, "Processed %i lines." % line_num
      print "Processed %i lines." % line_num

  print >> error_stream, "Finished writing to local files."

  # close out the local files
  for fragment_value, file_descriptor in file_descriptors.iteritems():
    print >> error_stream, "Closing file descriptor %s" % fragment_value

    # close file
    file_descriptor["file"].close()

    # copy files to output folder in HDFS
    if output_path != '':

      hdfs_fragment_path = output_path + '/' + fragment_value + '/'

      # create HDFS output folder if necessary
      if output_path != '':
        print "Creating HDFS fragment path %s if necessary" % hdfs_fragment_path
        execute("hadoop fs -mkdir -p %s" % hdfs_fragment_path)

      execute("hadoop fs -copyFromLocal %s %s" % (file_descriptor["file_name"], hdfs_fragment_path))

    # write fragment values to mongodb
    print >> output_stream, "Adding fragment value %s to mongodb." % (fragment_value)
    mongo_schema_collection.update({"type": "fragments"}, {"$addToSet": {"fragments": fragment_value}}, upsert = True);

  for shard_value in shard_values:
    # write shard values to mongodb
    if shard_key is not None:
      print >>  output_stream, "Adding shard value %s to mongodb." % (shard_value)
      mongo_schema_collection.update({"type": "shards"}, {"$addToSet": {"shards": shard_value}}, upsert = True);


if __name__ == "__main__":
  main(sys.argv[1:])

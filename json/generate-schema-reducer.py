#!/usr/bin/env python

#
# Copyright 2015, OneFold
# All rights reserved.
# http://www.onefold.io
#
# Author: Jorge Chang
#
# See license in LICENSE file.
#
# Generate Schema Reducer - reduces multiple / conflicting data type of a particular
# field into the most general one, e.g.
# input: "zip_code" => (int, string)
# output: "zip_code" => (string) because string > int.
#

import sys
import codecs
from pymongo import MongoClient

# create utf reader and writer for stdin and stdout
output_stream = codecs.getwriter("utf-8")(sys.stdout)
input_stream = codecs.getreader("utf-8")(sys.stdin, errors="ignore")
error_stream = codecs.getwriter("utf-8")(sys.stderr)

mongo_schema_collection = None


def parse_datatype_mode (datatype_mode):
  a = datatype_mode.split("-")
  if len(a) >= 2:
    return (a[0], a[1])
  else:
    raise ValueError('Invalid datatype / mode tuple %s' % datatype_mode)


def process_new_field(key, datatype_mode):

  if key is not None and datatype_mode is not None:
    # check if key is already in mongodb
    orig_field_record = mongo_schema_collection.find_one({"key": key, "type": "field"})

    # compare orig data type and save schema to mongodb
    if orig_field_record is not None:
      orig_datatype_mode = orig_field_record['data_type'] + "-" + orig_field_record['mode']

      forced = False
      if 'forced' in orig_field_record and orig_field_record['forced'] == True:
        forced = True

      # if 'forced' not in orig_datatype:
      if not forced:
        new_datatype_mode = max_datatype_mode(orig_datatype_mode, datatype_mode)

        (new_datatype, new_mode) = parse_datatype_mode(new_datatype_mode)
        mongo_schema_collection.find_one_and_update({"key": key, "type": "field"},
                                                    {"$set": {"data_type": new_datatype,
                                                              "mode": new_mode}})

    else:
      (datatype, mode) = parse_datatype_mode(datatype_mode)
      mongo_schema_collection.insert_one({"key": key,
                                          "type": "field",
                                          "data_type": datatype,
                                          "mode": mode})


def max_datatype_mode (datatype_mode_1, datatype_mode_2):

  if datatype_mode_1 == datatype_mode_2:
    return datatype_mode_1

  if datatype_mode_1 == 'record-repeated' or datatype_mode_2 == 'record-repeated':
    return 'record-repeated'

  if datatype_mode_1 == 'string-repeated' or datatype_mode_2 == 'string-repeated':
    return 'string-repeated'

  if datatype_mode_1 == 'repeated-nullable' or datatype_mode_2 == 'repeated-nullable':
    return 'repeated-nullable'

  if datatype_mode_1 == 'record-nullable' or datatype_mode_2 == 'record-nullable':
    return 'record-nullable'

  if datatype_mode_1 == 'string-nullable' or datatype_mode_2 == 'string-nullable':
    return 'string-nullable'

  if datatype_mode_1 == 'float-nullable' and datatype_mode_2 == 'integer-nullable':
    return 'float-nullable'

  if datatype_mode_1 == 'integer-nullable' and datatype_mode_2 == 'float-nullable':
    return 'float-nullable'

  return 'string-nullable'


def usage():
  print "Usage: %s mongodb://[host]:[port]/[db_name]/[schema_collection_name]" % sys.argv[0]
  sys.exit(2)


def main(argv):

  if len(argv) < 0:
    usage()

  try:

    args = argv[0].split("/")
    schema_collection_name = args[-1]
    schema_db_name = args[-2]
    mongo_uri = '/'.join(args[0:-2])

    client = MongoClient(mongo_uri)
    db = client[schema_db_name]

    global mongo_schema_collection
    mongo_schema_collection = db[schema_collection_name]

  except:
    usage()

  current_key = None
  current_datatype_mode = None
  key = None

  # input comes from STDIN
  for line in input_stream:

    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    (key, datatype_mode) = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: key) before it is passed to the reducer
    if current_key == key:
      current_datatype_mode = max_datatype_mode(current_datatype_mode, datatype_mode)
    else:
      if current_key:
        process_new_field(current_key, current_datatype_mode)
      current_datatype_mode = datatype_mode
      current_key = key

  # do not forget to output the last key if needed!
  if current_key == key:
    process_new_field(current_key, current_datatype_mode)


if __name__ == "__main__":
  main(sys.argv[1:])

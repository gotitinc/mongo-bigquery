#!/usr/bin/env python

import sys
import codecs
from pymongo import MongoClient

# create utf reader and writer for stdin and stdout
output_stream = codecs.getwriter("utf-8")(sys.stdout)
input_stream = codecs.getreader("utf-8")(sys.stdin, errors="ignore")
error_stream = codecs.getwriter("utf-8")(sys.stderr)

mongo_schema_collection = None


def process_new_field(key, datatype):

  if key is not None and datatype is not None:
    # check if key is already in mongodb
    orig_datatype_record = mongo_schema_collection.find_one({"key": key, "type": "field"})

    # compare orig data type and save schema to mongodb
    if orig_datatype_record is not None:
      orig_datatype = orig_datatype_record['data_type']
      if 'forced' not in orig_datatype:
        new_datatype = max_datatype(orig_datatype, datatype)
        print key, new_datatype
        mongo_schema_collection.find_one_and_update({"key": key, "type": "field"}, {"$set": {"data_type": new_datatype}})

    else:
      print key, datatype
      mongo_schema_collection.insert_one({"key": key, "type": "field", "data_type": datatype})


def max_datatype (datatype1, datatype2):

  if datatype1 == datatype2:
    return datatype1

  if datatype1 == 'record-repeated' or datatype2 == 'record-repeated':
    return 'record-repeated'

  if datatype1 == 'string-repeated' or datatype2 == 'string-repeated':
    return 'string-repeated'

  if datatype1 == 'repeated-nullable' or datatype2 == 'repeated-nullable':
    return 'repeated-nullable'

  if datatype1 == 'record-nullable' or datatype2 == 'record-nullable':
    return 'record-nullable'

  if datatype1 == 'string-nullable' or datatype2 == 'string-nullable':
    return 'string-nullable'

  if datatype1 == 'float-nullable' and datatype2 == 'integer-nullable':
    return 'float-nullable'

  if datatype1 == 'integer-nullable' and datatype2 == 'float-nullable':
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
  current_datatype = None
  key = None

  # input comes from STDIN
  for line in input_stream:

    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    (key, datatype) = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: key) before it is passed to the reducer
    if current_key == key:
      current_datatype = max_datatype(current_datatype, datatype)
    else:
      if current_key:
        process_new_field(current_key, current_datatype)
      current_datatype = datatype
      current_key = key

  # do not forget to output the last key if needed!
  if current_key == key:
    process_new_field(current_key, current_datatype)


if __name__ == "__main__":
  main(sys.argv[1:])

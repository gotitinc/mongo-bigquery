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
# Generate Schema Mapper - takes data from stdin, performs deep inspection and emits
# field-name -> data-type tuples.
#

import re
import sys
import json
import codecs

# create utf reader and writer for stdin and stdout
output_stream = codecs.getwriter("utf-8")(sys.stdout)
input_stream = codecs.getreader("utf-8")(sys.stdin, errors="ignore")
error_stream = codecs.getwriter("utf-8")(sys.stderr)

def is_integer(value):
  try:
    a = int(str(value))
    if a > sys.maxint or a < -sys.maxint - 1:
      return False
    return True
  except:
    return False

def is_float(value):
  try:
    float(str(value))
    return True
  except:
    return False

def process_line(line, line_num, parent=None, seperator="_"):

  # parse the line
  try:
    data = json.loads(line, encoding='utf-8')
  except ValueError:
    print >> error_stream, "Line %i: JSON Parse Error. Data: %s" % (line_num, line)
    return

  if data:

    for key, value in data.iteritems():

      k = re.sub("[^0-9a-zA-Z_]", '_', key).lower()

      # BigQuery disallows field to start with non alpha
      if ord(k[0]) >= 48 and ord(k[0]) <= 59:
        k = "_f" + k

      # Hive disallows field to start with "_"
      if k[0] == '_':
        k = k.lstrip("_")

      if parent == None:
        full_key = k
      else:
        full_key = parent + seperator + k

      if value is None:
        # if data is Null, PASS.
        pass

      elif isinstance(value, dict):

        if len(value) > 0:
          print >> output_stream, "%s\t%s" % (full_key, "record-nullable")
          process_line(json.dumps(value, ensure_ascii=False), line_num, full_key)
        else:
          print >> error_stream, "Key %s has value of type dict %s which is empty. Ignoring." % (full_key, value)

      elif isinstance(value, list):

        if len(value) > 0:

          for list_value in value:
            if isinstance(list_value, dict):
              print >> output_stream, "%s\t%s" % (full_key, "record-repeated")
              process_line(json.dumps(list_value, ensure_ascii=False), line_num, full_key, ".")
            elif isinstance(list_value, bool):
              print >> output_stream, "%s\t%s" % (full_key, "boolean-repeated")
            elif isinstance(list_value, int):
              print >> output_stream, "%s\t%s" % (full_key, "integer-repeated")
            elif isinstance(list_value, float):
              print >> output_stream, "%s\t%s" % (full_key, "float-repeated")
            else:
              print >> output_stream, "%s\t%s" % (full_key, "string-repeated")

        else:
          print >> error_stream, "Key %s has value of type list %s which is empty. Ignoring." % (full_key, value)

      else:

        if isinstance(value, bool):
          print >> output_stream, "%s\t%s" % (full_key, "boolean-nullable")
        elif isinstance(value, int):
          print >> output_stream, "%s\t%s" % (full_key, "integer-nullable")
        elif isinstance(value, float):
          print >> output_stream, "%s\t%s" % (full_key, "float-nullable")
        else:
          print >> output_stream, "%s\t%s" % (full_key, "string-nullable")


def main():

  line_num = 1
  for line in input_stream:
    try:
      process_line(line, line_num, None)
      line_num += 1
    except Exception:
      print >> error_stream, "Line %i: Error. Data: %s" % (line_num, line)

if __name__ == "__main__":
  main()
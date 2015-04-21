#!/usr/bin/env python

import sys
import traceback
import codecs

# create utf reader and writer for stdin and stdout
output_stream = codecs.getwriter("utf-8")(sys.stdout)
input_stream = codecs.getreader("utf-8")(sys.stdin, errors="ignore")
error_stream = codecs.getwriter("utf-8")(sys.stderr)


def process_line(line, line_num, app_id, parent=None):
  # remove '\n'
  line = line.strip()

  try:
    print >> output_stream, line
  except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    backtrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
    print >> error_stream, 'Line Number: %s' % str(line_num)
    print >> error_stream, 'Line: %s' % line
    print >> error_stream, ''.join(backtrace)


def usage():
  print "Usage: %s [env],[app-id]" % sys.argv[0]
  sys.exit(2)


def main(argv):
  if len(argv) < 1:
    usage()

  try:
    args = argv[0].split(",")
    env = args[0]
    app_id = args[1]
    gs_output_path = args[2]
  except ValueError:
    usage()

  if app_id is None:
    usage()

  # process input
  line_num = 1
  for line in input_stream:
    # process line
    process_line(line, line_num, app_id, None)
    line_num += 1


if __name__ == "__main__":
  main(sys.argv[1:])

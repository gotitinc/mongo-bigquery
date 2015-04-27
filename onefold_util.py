import subprocess
import os
import random
import time

# execute shell command
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


def execute_and_read_with_retry(command):
  for n in range(0,5):
    (return_code, stdout_lines, stderr_lines) = execute_and_read(command)
    if return_code == 0:
      break
    else:
      print "Error executing command: %s with return code %s" % (command, return_code)
      print 'Retry-able. Sleeping...'
      time.sleep((2 ** n) + random.randint(0, 1000) / 1000)

  return (return_code, stdout_lines, stderr_lines)


# execute shell command and return stdout as list of strings
def execute_and_read(command):
  # run command and read stdout
  print 'Executing command: %s' % command
  p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  p.wait()

  return_code = p.returncode

  stdout_lines = p.stdout.readlines()
  # print stdout
  for line in stdout_lines:
    print line.strip()

  stderr_lines = p.stderr.readlines()
  # print stderr
  for line in stderr_lines:
    print line.strip()

  return (return_code, stdout_lines, stderr_lines)



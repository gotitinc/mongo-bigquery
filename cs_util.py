#!/usr/bin/env python

#
# Author: Raghav Rastogi
#
# Cloud Storage utility - abstraction of all cloud storage related calls.
# Implementation for Hadoop and Google Cloud Storage provided. Basic functionaliy
# like mkdir, rmdir and copy_from_local.
#

from onefold_util import execute

class CloudStorage:
    
    def rmdir(self, path):
        return 

    def mkdir(self, path):
        return
    
    def copy_from_local(self, source_local_file_path, dest_path):
        return


# HDFS implementation.
class HDFSStorage(CloudStorage):
    
    def rmdir(self, path):
        execute("hadoop fs -rm -r -f %s" % path, ignore_error=True)

    def mkdir(self, path):
        execute("hadoop fs -mkdir -p %s" % path, ignore_error=True)
    
    def copy_from_local(self, source_local_file_path, dest_path):
        execute("hadoop fs -copyFromLocal %s %s/" % (source_local_file_path, dest_path))
        

# Google Cloud Storage implementation.
class GCloudStorage(CloudStorage):
    
    project_id = None
    bucket_id = None
    
    def __init__(self, project_id, bucket_id):
        self.project_id = project_id
        self.bucket_id = bucket_id

    def rmdir(self, path):
        
        print 'rmdir: %s' % (path)

        if not path.endswith("/"):
            path = path + "/"
        
        command = "gsutil -m rm -rf gs://%s/%s" % (self.bucket_id, path)
        execute(command, ignore_error=True)

    def mkdir(self, path):
        # nothing to do. there are no folders in google cloud storage
        pass
    
    def copy_from_local(self, source_local_file_path, dest_path):

        print 'copy_from_local: %s %s' % (source_local_file_path, dest_path)
        
        if not dest_path.endswith("/"):
            dest_path = dest_path + "/"
            
        dest_path = dest_path + source_local_file_path.split("/")[-1]
        
        command = "gsutil -m cp %s gs://%s/%s" % (source_local_file_path, self.bucket_id, dest_path)
        execute(command, ignore_error=False, retry=True)

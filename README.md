# Mongo - Google Big Query Connector

Super-easy way to load your MongoDB collection into Google BigQuery. The code creates Google BigQuery schema automatically by performing a deep inspection of each MongoDB record and deriving the data type of each field. Supports basic data types, nested objects, array of primitive data types and array of objects.

Nested fields are flattened out into columns.

Arrays are typically split into a different (child) BigQuery table with parent/child relationship with the root table.

## How it works

1. Connects to your MongoDB and extract the specified collection into local file which is then copied to Google Cloud Storage.
2. MapReduce generates schema (a copy is saved back to MongoDB for info).
3. MapReduce transforms data, breaking the array into multiple files in Google Cloud Storage output folder.
4. Create BigQuery tables using schema generated in step 2.
5. Load BigQuery tables using Google Cloud Storage files generated in step 3.

## Pre-requisites

1. You have a Hadoop cluster.
2. You can SSH to the master node.
3. Make sure `hadoop` program is in your `PATH`.
4. In each node, the following is installed:
  * python (2.6+)
  * pip
  * pymongo

If not, you can run the following on each node:
```
yum -y install epel-release
yum -y install python-pip
pip install pymongo
```

## Install

1. git clone this repo on the master node in your Hadoop cluster.
2. Run this to compile custom code needed for MapReduce:

  ```
  cd java/MapReduce
  mvn package
  ```
3. Make sure you have gcloud command line utilities installed in your Hadoop master mode. Executables that this program depends on are:

  ```
  gsutil
  bq
  ```

In `onefold.py`, near the top, there are a few configuration that you can customized. Make sure these variables are set correctly before proceeding.

`TMP_PATH`

Where the script will store extracted data from MongoDB.

`CLOUD_STORAGE_PATH`

Google Cloud Storage Path where it will store files for MapReduce and BigQuery.


## Usage

### Simple case
Say you have a MongoDB collection called "test.users", and you have some records in it:

```
> db.users.find();
{ "_id" : ObjectId("5688d0855d53fc2c133f3429"), "mobile" : { "carrier" : "Sprint", "device" : "Samsung" }, "name" : "John Doe", "age" : 24, "utm_campaign" : "Facebook_Offer", "app_version" : "2.4", "address" : { "city" : "Chicago", "zipcode" : 94012 } }
```

To load this into BigQuery,

```
./onefold.py --mongo mongodb://[mongodb_host]:[mongodb_port] \
             --source_db test \
             --source_collection users \
             --infra_type gcloud \
             --dest_db_name test \
             --dest_table_name users \
             --gcloud_project_id [google_cloud_project_id] \
             --gcloud_storage_bucket_id [google_cloud_storage_bucket_id]
```

Results:
```
-- Initializing Google BigQuery module --
Creating file /tmp/onefold_mongo/users/data/1
Executing command: cat /tmp/onefold_mongo/users/data/1 | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py mongodb://localhost:27017/test/users_schema > /dev/null
Executing command: cat /tmp/onefold_mongo/users/data/1 | json/transform-data-mapper.py mongodb://localhost:27017/test/users_schema,/tmp/onefold_mongo/users/data_transform/output > /dev/null
...
Executing command: gsutil -m rm -rf gs://mongo-gbq-bucket/onefold_mongo/users/data_transform/output/
Removing gs://mongo-gbq-bucket/onefold_mongo/users/data_transform/output/root/part-00000#1451806915461000...
copy_from_local: /tmp/onefold_mongo/users/data_transform/output/root/part-00000 onefold_mongo/users/data_transform/output/root/
Executing command: gsutil -m cp /tmp/onefold_mongo/users/data_transform/output/root/part-00000 gs://mongo-gbq-bucket/onefold_mongo/users/data_transform/output/root/part-00000
...
Executing command: bq --project_id mongo-gbq --format csv ls test
Executing command: bq --project_id mongo-gbq mk --schema users_schema.json test.users
Table 'mongo-gbq:test.users' successfully created.
Loading fragment: root
Executing command: bq --project_id mongo-gbq --nosync load --source_format NEWLINE_DELIMITED_JSON test.users gs://mongo-gbq-bucket/onefold_mongo/users/data_transform/output/root/*
Successfully started load mongo-gbq:bqjob_r4d275de6da77baf3_0000015206702df7_1
-------------------
    RUN SUMMARY
-------------------
Num records extracted 1
Num records rejected 0
Extracted data with _id from 5688d0855d53fc2c133f3429 to 5688d0855d53fc2c133f3429
Extracted files are located at: /tmp/onefold_mongo/users/data/1
Destination Tables: users
Schema is stored in Mongo test.users_schema
```

In Google BigQuery, you can see:
```
$ bq show test.users
Table mongo-gbq:test.users

   Last modified             Schema             Total Rows   Total Bytes   Expiration
 ----------------- --------------------------- ------------ ------------- ------------
  02 Jan 23:43:12   |- address_city: string     1            141
                    |- address_zipcode: float
                    |- age: float
                    |- app_version: string
                    |- id_oid: string
                    |- mobile_carrier: string
                    |- mobile_device: string
                    |- name: string
                    |- utm_campaign: string
                    |- hash_code: string

$ bq query "select * from test.users"
Waiting on bqjob_r710f4e875a413367_000001520674ebba_1 ... (0s) Current status: DONE
+--------------+-----------------+------+-------------+--------------------------+----------------+---------------+----------+----------------+------------------------------------------+
| address_city | address_zipcode | age  | app_version |          id_oid          | mobile_carrier | mobile_device |   name   |  utm_campaign  |                hash_code                 |
+--------------+-----------------+------+-------------+--------------------------+----------------+---------------+----------+----------------+------------------------------------------+
| Chicago      |         94012.0 | 24.0 | 2.4         | 5688d0855d53fc2c133f3429 | Sprint         | Samsung       | John Doe | Facebook_Offer | abf9a2ac1ce71feb12418c889b913f8d8361a6d4 |
+--------------+-----------------+------+-------------+--------------------------+----------------+---------------+----------+----------------+------------------------------------------+
```

In Mongo, you can see the schema saved in a collection called `users_schema`:
```
> db.users_schema.find();
{ "_id" : ObjectId("55426ae6296e827fc79300b1"), "type" : "field", "data_type" : "string-nullable", "key" : "address_city" }
{ "_id" : ObjectId("55426ae6296e827fc79300b2"), "type" : "field", "data_type" : "record-nullable", "key" : "address" }
{ "_id" : ObjectId("55426ae6296e827fc79300b3"), "type" : "field", "data_type" : "integer-nullable", "key" : "address_zipcode" }
{ "_id" : ObjectId("55426ae6296e827fc79300b4"), "type" : "field", "data_type" : "integer-nullable", "key" : "age" }
{ "_id" : ObjectId("55426ae6296e827fc79300b5"), "type" : "field", "data_type" : "string-nullable", "key" : "app_version" }
{ "_id" : ObjectId("55426ae6296e827fc79300b6"), "type" : "field", "data_type" : "string-nullable", "key" : "id_oid" }
{ "_id" : ObjectId("55426ae6296e827fc79300b7"), "type" : "field", "data_type" : "record-nullable", "key" : "id" }
{ "_id" : ObjectId("55426ae6296e827fc79300b8"), "type" : "field", "data_type" : "string-nullable", "key" : "mobile_carrier" }
{ "_id" : ObjectId("55426ae6296e827fc79300b9"), "type" : "field", "data_type" : "string-nullable", "key" : "mobile_device" }
{ "_id" : ObjectId("55426ae6296e827fc79300ba"), "type" : "field", "data_type" : "record-nullable", "key" : "mobile" }
{ "_id" : ObjectId("55426ae6296e827fc79300bb"), "type" : "field", "data_type" : "string-nullable", "key" : "name" }
{ "_id" : ObjectId("55426ae6296e827fc79300bc"), "type" : "field", "data_type" : "string-nullable", "key" : "utm_campaign" }
{ "_id" : ObjectId("55426ae72e2ecef82b7417d1"), "type" : "fragments", "fragments" : [ "root" ] }
```

Notes:

1. By default, extracted data is saved in `/tmp/onefold_mongo`. It can be changed by specifying the `tmp_path` parameter.
2. If `--use_mr` parameter is specified, it will use MapReduce to generate schema and transform data. Otherwise, it runs the mapper and reducer via command line using `cat [input] | mapper | sort | reducer` metaphor. This is handy if you don't have many records and/or just want to get this going quickly.
3. The generated files are in JSON format.
4. Nested objects like `mobile` and `address` in the above example are flattened out in the BigQuery table.
5. `hash_code` column is added. It's basically an SHA1 hash of the object. It's useful later on when we use `hash_code` as parent-child key to represent array in a child table.


### Now let's try a more complex collection.

In Mongo, create a `complex_users` collection with the following fields:
```
> db.complex_users.find()
{ "_id" : ObjectId("5688d73c5d53fc2c133f342b"), "hobbies" : [ "reading", "cycling" ], "age" : 34, "work_history" : [ { "to" : "present", "from" : 2013, "name" : "IBM" }, { "to" : 2013, "from" : 2003, "name" : "Bell" } ], "utm_campaign" : "Google", "name" : "Alexander Keith", "app_version" : "2.5", "mobile" : { "device" : "iPhone", "carrier" : "Rogers" }, "address" : { "state" : "Ontario", "zipcode" : "M1K3A5", "street" : "26 Marshall Lane", "city" : "Toronto" } }
```

A new `hobbies` field is added that is a string array.
A new `work_history` field is added that is an array of nested objects.

Run the following command to load `complex_users` collection into BigQuery:
```
./onefold.py --mongo mongodb://[mongodb_host]:[mongodb_port] \
             --source_db test \
             --source_collection complex_users \
             --infra_type gcloud \
             --dest_db_name test \
             --dest_table_name complex_users \
             --gcloud_project_id [google_cloud_project_id] \
             --gcloud_storage_bucket_id [google_cloud_storage_bucket_id]
```

Results:
```
-- Initializing Google BigQuery module --
Creating file /tmp/onefold_mongo/complex_users/data/1
Executing command: cat /tmp/onefold_mongo/complex_users/data/1 | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py mongodb://localhost:27017/test/complex_users_schema > /dev/null
Executing command: cat /tmp/onefold_mongo/complex_users/data/1 | json/transform-data-mapper.py mongodb://localhost:27017/test/complex_users_schema,/tmp/onefold_mongo/complex_users/data_transform/output > /dev/null
Executing command: rm -rf /tmp/onefold_mongo/complex_users/data_transform/output
Executing command: mkdir -p /tmp/onefold_mongo/complex_users/data_transform/output/root
Opening file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/root/part-00000
Opened file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/root/part-00000
Executing command: mkdir -p /tmp/onefold_mongo/complex_users/data_transform/output/work_history
Opening file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/work_history/part-00000
Opened file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/work_history/part-00000
Executing command: mkdir -p /tmp/onefold_mongo/complex_users/data_transform/output/hobbies
Opening file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000
Opened file descriptor /tmp/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000
...
Executing command: gsutil -m rm -rf gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/
copy_from_local: /tmp/onefold_mongo/complex_users/data_transform/output/root/part-00000 onefold_mongo/complex_users/data_transform/output/root/
Executing command: gsutil -m cp /tmp/onefold_mongo/complex_users/data_transform/output/root/part-00000 gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/root/part-00000
Copying file:///tmp/onefold_mongo/complex_users/data_transform/output/root/part-00000 [Content-Type=application/octet-stream]...
copy_from_local: /tmp/onefold_mongo/complex_users/data_transform/output/work_history/part-00000 onefold_mongo/complex_users/data_transform/output/work_history/
Executing command: gsutil -m cp /tmp/onefold_mongo/complex_users/data_transform/output/work_history/part-00000 gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/work_history/part-00000
Copying file:///tmp/onefold_mongo/complex_users/data_transform/output/work_history/part-00000 [Content-Type=application/octet-stream]...
copy_from_local: /tmp/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000 onefold_mongo/complex_users/data_transform/output/hobbies/
Executing command: gsutil -m cp /tmp/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000 gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000
Copying file:///tmp/onefold_mongo/complex_users/data_transform/output/hobbies/part-00000 [Content-Type=application/octet-stream]...
...
Executing command: bq --project_id mongo-gbq mk --schema complex_users_schema.json test.complex_users
Table 'mongo-gbq:test.complex_users' successfully created.
Executing command: bq --project_id mongo-gbq mk --schema complex_users_work_history_schema.json test.complex_users_work_history
Table 'mongo-gbq:test.complex_users_work_history' successfully created.
Executing command: bq --project_id mongo-gbq mk --schema complex_users_hobbies_schema.json test.complex_users_hobbies
Table 'mongo-gbq:test.complex_users_hobbies' successfully created.
Loading fragment: root
Executing command: bq --project_id mongo-gbq --nosync load --source_format NEWLINE_DELIMITED_JSON test.complex_users gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/root/*
Successfully started load mongo-gbq:bqjob_r4fe3384c09234c1d_00000152068b5e85_1
Loading fragment: work_history
Executing command: bq --project_id mongo-gbq --nosync load --source_format NEWLINE_DELIMITED_JSON test.complex_users_work_history gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/work_history/*
Successfully started load mongo-gbq:bqjob_r138de33f6e2058cc_00000152068b62aa_1
Loading fragment: hobbies
Executing command: bq --project_id mongo-gbq --nosync load --source_format NEWLINE_DELIMITED_JSON test.complex_users_hobbies gs://mongo-gbq-bucket/onefold_mongo/complex_users/data_transform/output/hobbies/*
Successfully started load mongo-gbq:bqjob_r361aa8424636d4a0_00000152068b689e_1
-------------------
    RUN SUMMARY
-------------------
Num records extracted 1
Num records rejected 0
Extracted data with _id from 5688d73c5d53fc2c133f342b to 5688d73c5d53fc2c133f342b
Extracted files are located at: /tmp/onefold_mongo/complex_users/data/1
Destination Tables: complex_users complex_users_work_history complex_users_hobbies
Schema is stored in Mongo test.complex_users_schema
```

In BigQuery, three new tables are created: `complex_users`, `complex_users_hobbies` and `complex_users_work_history`
```
$ bq ls test
           tableId             Type
 ---------------------------- -------
  complex_users                TABLE
  complex_users_hobbies        TABLE
  complex_users_work_history   TABLE

$ bq show test.complex_users
Table mongo-gbq:test.complex_users

   Last modified              Schema             Total Rows   Total Bytes   Expiration
 ----------------- ---------------------------- ------------ ------------- ------------
  03 Jan 00:12:48   |- address_city: string      1            166
                    |- address_state: string
                    |- address_street: string
                    |- address_zipcode: string
                    |- age: float
                    |- app_version: string
                    |- id_oid: string
                    |- mobile_carrier: string
                    |- mobile_device: string
                    |- name: string
                    |- utm_campaign: string
                    |- hash_code: string

$ bq show test.complex_users_hobbies
Table mongo-gbq:test.complex_users_hobbies

   Last modified              Schema              Total Rows   Total Bytes   Expiration
 ----------------- ----------------------------- ------------ ------------- ------------
  03 Jan 00:12:49   |- parent_hash_code: string   2            102
                    |- hash_code: string
                    |- value: string

$ bq show test.complex_users_work_history
Table mongo-gbq:test.complex_users_work_history

   Last modified              Schema              Total Rows   Total Bytes   Expiration
 ----------------- ----------------------------- ------------ ------------- ------------
  03 Jan 00:12:47   |- parent_hash_code: string   2            212
                    |- hash_code: string
                    |- from: float
                    |- name: string
                    |- to: string
```

You can join parent and child table like:
```
$ bq query "select * from test.complex_users join test.complex_users_hobbies on test.complex_users.hash_code = test.complex_users_hobbies.parent_hash_code"
```

## Parameters

`--mongo`
MongoDB connectivity URI, e.g. mongodb://127.0.0.1:27017

`--source_db`
The MongoDB database name from which to extract data.

`--source_collection`
The MongoDB collection name from which to extract data.

`--query`
Optional query users can specify when doing extraction. Useful for filtering out only incremental records. See below for some examples.

`--tmp_path`
Optional. Path used to store extracted data. Default is `/tmp/onefold_mongo`

`--schema_db`
Optional. The MongoDB database name to which schema data is written. Default to the same database as source.

`--schema_collection`
Optional. The MongoDB collection to which schema data is written. Default to `[source_collection]_schema`.

`--dest_db_name`
Optional. The BigQuery dataset to use.

`--dest_table_name`
Optional. The BigQuery table name to use. If not specified, it will use source collection name.

`--use_mr`
If this parameter is specified, the program will use MapReduce to generate schema and transform data. If not, the mapper and reducer will be executed as command line using the `cat [input] | mapper | sort | reducer` metaphore. This is useful for small data set and if you just want to get things up and running quickly.

`--policy_file`
Use the specified file for policies which you can use to configure required fields, etc. See below for supported policies

`--infra_type`
Specify `gcloud` for Google BigQuery

`--gcloud_project_id`
Specify the Google Cloud project id

`--gcloud_storage_bucket_id`
Specify the bucket ID of the Google Cloud Storage bucket to use for file storage


## Policy Manager

Policy manager is used to control schema generation. With the policy manager, you can:

1. Specify required fields. If the field is missing, the document is rejected. Rejected documents are saved in `[TMP_PATH]/[collection_name]/rejected` folder.
2. Enforce data type for certain fields. In the example below, `age` is forced to be integer. So if there is a document that contains non-integer, the field will be null.

Example policy file:

```
[
    {
        "key": "last_name",
        "required": true
    },
    {
        "key": "address.zipcode",
        "data_type": "integer"
    }
]
```

Save the policy file, and pass the policy file in as command line argument via `--policy_file`.


## Query Examples

To query for charge_id > 1237489:
```
--query '{"charge_id":{"$gt":1237489}}'
```

To query for _id > 55401a60151a4b1a4f000001:
```
--query '{"_id": {"$gt":ObjectId("55401a60151a4b1a4f000001")}}'
```

## Known Issues

* There is no easy way to capture records that were updated in MongoDB. We are working on capturing oplog and replay inserts and updates.
* The ways in which the data type of a given changes over time is huge. A field can change from an int, to a string, to an array of string, to an array of mix types, to an array of complex objects over time. We haven't tested all the different combinations, but very interested in support as many as we can. Let us know if you have found a case that we don't support well.
* Currently since BigQuery doesn't support alter-table, we can only support `overwrite` mode.


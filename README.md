# Mongo - Hive Connector

Super-easy way to load your MongoDB collection into Hive. The code creates Hive schema automatically by performing a deep inspection of each MongoDB record and deriving the data type of each field. Supports basic data types, nested objects, array of primitive data types and array of objects.

Nested fields are flattened out into columns.

Arrays are typically split into a different (child) Hive table with parent/child relationship with the root table.

## How it works

1. Connects to your MongoDB and extract the specified collection into local file which is then copied to HDFS.
2. MapReduce generates schema (a copy is saved back to MongoDB for info).
3. MapReduce transforms data, breaking the array into multiple files in HDFS output folder.
4. Create Hive tables using schema generated in step 2.
5. Load Hive tables using HDFS files generated in step 3.

## Pre-requisites

1. You have a Hadoop cluster.
2. You can SSH to the master node.
3. In each node, the following is installed:
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
3. Install Python pyhs2 package to connect to Hive Server:
```
yum install gcc-c++
yum install cyrus-sasl-devel.x86_64
yum install python-devel.x86_64
pip install pyhs2
```

In `onefold.py`, near the top, there are a few configuration that you can customized. Make sure these variables are set correctly before proceeding.

`TMP_PATH`

Where the script will store extracted data from MongoDB.

`HDFS_PATH`

HDFS Path where it will store files for MapReduce and Hive.

`HADOOP_MAPREDUCE_STREAMING_LIB`

Make sure this points to a valid `hadoop-streaming.jar`. The default value is set for Hortonworks HDP 2.2.


## Usage

### Simple case
Say you have a MongoDB collection called "test.users", and you have some records in it:

```
> db.users.find();
{ "_id" : ObjectId("55426ac7151a4b4d32000001"), "mobile" : { "carrier" : "Sprint", "device" : "Samsung" }, "name" : "John Doe", "age" : 24, "utm_campaign" : "Facebook_Offer", "app_version" : "2.4", "address" : { "city" : "Chicago", "zipcode" : 94012 } }
```

To load this into Hive,

```
./onefold.py --mongo mongodb://[mongodb_host]:[mongodb_port] \
             --source_db test \
             --source_collection users \
             --hiveserver_host [hive_server_host] \
             --hiveserver_port [hive_server_port]
```

Results:
```
-- Initializing Hive Util --
Creating file /tmp/onefold_mongo/users/data/1
Executing command: cat /tmp/onefold_mongo/users/data/1 | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py mongodb://xxx:xxx/test/users_schema > /dev/null
Executing command: cat /tmp/onefold_mongo/users/data/1 | json/transform-data-mapper.py mongodb://xxx:xxx/test/users_schema,/tmp/onefold_mongo/users/data_transform/output > /dev/null
...
Executing command: hadoop fs -mkdir -p onefold_mongo/users/data_transform/output/root
Executing command: hadoop fs -copyFromLocal /tmp/onefold_mongo/users/data_transform/output/root/part-00000 onefold_mongo/users/data_transform/output/root/
..
Executing HiveQL: show tables
Executing HiveQL: create table users (app_version string,utm_campaign string,id_oid string,age int,mobile_device string,name string,address_city string,hash_code string,mobile_carrier string,address_zipcode int) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
Executing HiveQL: load data inpath 'onefold_mongo/users/data_transform/output/root/*' into table users
-------------------
    RUN SUMMARY
-------------------
Extracted data with _id from 55426ac7151a4b4d32000001 to 55426ac7151a4b4d32000001
Extracted files are located at: /tmp/onefold_mongo/users/data/1
Hive Tables: users
Schema is stored in Mongo test.users_schema
```

In Hive, you can see:
```
hive> add jar [install_path]/hive-serdes-1.0-SNAPSHOT.jar;
hive> desc users;
app_version             string                  from deserializer
utm_campaign            string                  from deserializer
id_oid                  string                  from deserializer
age                     int                     from deserializer
mobile_device           string                  from deserializer
name                    string                  from deserializer
address_city            string                  from deserializer
hash_code               string                  from deserializer
mobile_carrier          string                  from deserializer
address_zipcode         int                     from deserializer
Time taken: 0.073 seconds, Fetched: 10 row(s)
hive> select * from users;
2.4     Facebook_Offer  55426ac7151a4b4d32000001        24      Samsung John Doe        Chicago 863a4ddd10579c8fc7e12b5bd1e188ce083eec2d        Sprint  94012
Time taken: 0.07 seconds, Fetched: 1 row(s)
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
3. The generated HDFS files are in JSON format, so in Hive, you need to add the included JSON Serde. In Hive, run this command before select from the generated tables: `add jar [install_path]/hive-serdes-1.0-SNAPSHOT.jar`
4. Nested objects like `mobile` and `address` in the above example are flattened out in the Hive table.
5. `hash_code` column is added. It's basically an SHA1 hash of the object. It's useful later on when we use `hash_code` as parent-child key to represent array in a child table.


### Now let's add a record with new fields

In Mongo, one new records is added with some new fields:
```
> db.users.find();
...
{ "_id" : ObjectId("55426c42151a4b4d9e000001"), "hobbies" : [ "reading", "cycling" ], "age" : 34, "work_history" : [ { "to" : "present", "from" : 2013, "name" : "IBM" }, { "to" : 2013, "from" : 2003, "name" : "Bell" } ], "utm_campaign" : "Google", "name" : "Alexander Keith", "app_version" : "2.5", "mobile" : { "device" : "iPhone", "carrier" : "Rogers" }, "address" : { "state" : "Ontario", "zipcode" : "M1K3A5", "street" : "26 Marshall Lane", "city" : "Toronto" } }
```
New fields added to `address` nested object.
`address.zipcode` is now string (used to be integer).
A new `hobbies` field is added that is a string array.
A new `work_history` field is added that is an array of nested objects.

Run the command with parameters `--write_disposition append` and `--query '{"_id":{"$gt":ObjectId("55426ac7151a4b4d32000001")}}'`:
```
./onefold.py --mongo mongodb://[mongodb_host]:[mongodb_port] \
             --source_db test \
             --source_collection users \
             --hiveserver_host [hive_server_host] \
             --hiveserver_port [hive_server_port]
             --write_disposition append \
             --query '{"_id":{"$gt":ObjectId("55426f15151a4b4e46000001")}}'
```

Results:
```
-- Initializing Hive Util --
...
Executing command: hadoop fs -mkdir -p onefold_mongo/users/data_transform/output/root
Executing command: hadoop fs -copyFromLocal /tmp/onefold_mongo/users/data_transform/output/root/part-00000 onefold_mongo/users/data_transform/output/root/
Executing command: hadoop fs -mkdir -p onefold_mongo/users/data_transform/output/work_history
Executing command: hadoop fs -copyFromLocal /tmp/onefold_mongo/users/data_transform/output/work_history/part-00000 onefold_mongo/users/data_transform/output/work_history/
Executing command: hadoop fs -mkdir -p onefold_mongo/users/data_transform/output/hobbies
Executing command: hadoop fs -copyFromLocal /tmp/onefold_mongo/users/data_transform/output/hobbies/part-00000 onefold_mongo/users/data_transform/output/hobbies/
...
Executing HiveQL: alter table `users` change `address_zipcode` `address_zipcode` string
Executing HiveQL: alter table `users` add columns (`address_state` string)
Executing HiveQL: alter table `users` add columns (`address_street` string)
Executing HiveQL: create table `users_hobbies` (parent_hash_code string,hash_code string,`value` string) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
Executing HiveQL: create table `users_work_history` (parent_hash_code string,hash_code string,`from` int,`name` string,`to` string) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
...
-------------------
    RUN SUMMARY
-------------------
Extracted data with _id from 55426f52151a4b4e5a000001 to 55426f52151a4b4e5a000001
Extracted files are located at: /tmp/onefold_mongo/users/data/1
Hive Tables: users users_hobbies users_work_history
Schema is stored in Mongo test.users_schema
```

In Hive, two new tables are created: `users_hobbies` and `users_work_history`
```
hive> show tables;
users
users_hobbies
users_work_history

hive> desc users_hobbies;
OK
parent_hash_code        string                  from deserializer
hash_code               string                  from deserializer
value                   string                  from deserializer
Time taken: 0.068 seconds, Fetched: 3 row(s)

hive> desc users_work_history;
OK
parent_hash_code        string                  from deserializer
hash_code               string                  from deserializer
from                    int                     from deserializer
name                    string                  from deserializer
to                      string                  from deserializer
Time taken: 0.067 seconds, Fetched: 5 row(s)
```

You can join parent and child table like:
```
hive> select * from users join users_hobbies on users.hash_code = users_hobbies.parent_hash_code
                          join users_work_history on users.hash_code = users_work_history.parent_hash_code;
```


## Parameters

`--mongo`
MongoDB connectivity URI, e.g. mongodb://127.0.0.1:27017

`--source_db`
The MongoDB database name from which to extract data.

`--source_collection`
The MongoDB collection name from which to extract data.

`--hiveserver_host`
Hive server host.

`--hiveserver_port`
Hive server port.

`--query`
Optional query users can specify when doing extraction. Useful for filtering out only incremental records. See below for some examples.

`--tmp_path`
Optional. Path used to store extracted data. Default is `/tmp/onefold_mongo`

`--schema_db`
Optional. The MongoDB database name to which schema data is written. Default to the same database as source.

`--schema_collection`
Optional. The MongoDB collection to which schema data is written. Default to `[source_collection]_schema`.

`--write_disposition`
Optional. Valid values are `overwrite` and `append`. Tells the program whether to overwrite the Hive table or to append to existing table.

`--hive_db_name`
Optional. The Hive database to use. If not specified, it will use `default` database.

`--hive_table_name`
Optional. The Hive table name to use. If not specified, it will use source collection name.

`--use_mr`
If this parameter is specified, the program will use MapReduce to generate schema and transform data. If not, the mapper and reducer will be executed as command line using the `cat [input] | mapper | sort | reducer` metaphore. This is useful for small data set and if you just want to get things up and running quickly.

`--policy_file`
Use the specified file for policies which you can use to configure required fields, etc. See below for supported policies

## Policy Manager

Policy manager is used to control schema generation. With the policy manager, you can:

1. Specify required fields.
2. Overwrite data type for certain fields.

Example policy file:

```
[
    {
        "field_name": "last_name",
        "required": true
    },
    {
        "field_name": "age",
        "data_type_overwrite": "int"
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

## FAQ

## Support

Email jorge@onefold.io.


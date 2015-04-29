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

|`TMP_PATH` | Where the script will store extracted data from MongoDB. |
| `HDFS_PATH` | HDFS Path where it will store files for MapReduce and Hive. |
|`HADOOP_MAPREDUCE_STREAMING_LIB` | Make sure this points to a valid `hadoop-streaming.jar`. The default value is set for Hortonworks HDP 2.2 |


## Usage

### Simple case
Say you have a MongoDB collection called "test.uber_events", and you have some records in it:

```
> db.uber_events.find();
{ "_id" : ObjectId("5541571c151a4b35b4000001"), "time" : 10000, "model" : "Galaxy S5", "device" : "Samsung", "carrier" : "Sprint", "distinct_id" : "14124820200", "utm_campaign" : "Facebook_Offer", "stripe_charge_id" : 1237489, "event" : "set_pickup_location", "stripe_customer_id" : 3748, "city" : "Chicago", "app_version" : "2.4" }
{ "_id" : ObjectId("5541571c151a4b35b4000002"), "time" : 10001, "model" : "Galaxy S5", "device" : "Samsung", "carrier" : "Sprint", "distinct_id" : "14124820201", "utm_campaign" : "Facebook_Offer", "stripe_charge_id" : 1237490, "event" : "set_pickup_location", "stripe_customer_id" : 3749, "city" : "Chicago", "app_version" : "2.4" }
{ "_id" : ObjectId("5541571c151a4b35b4000003"), "time" : 10002, "model" : "Galaxy S5", "device" : "Samsung", "carrier" : "Sprint", "distinct_id" : "14124820202", "utm_campaign" : "Facebook_Offer", "stripe_charge_id" : 1237491, "event" : "set_pickup_location", "stripe_customer_id" : 3750, "city" : "Chicago", "app_version" : "2.4" }
```

To load this into Hive,

```
./onefold.py --mongo mongodb://[mongodb_host]:[mongodb_port] \
             --source_db test \
             --source_collection uber_events \
             --hiveserver_host [hive_server_host] \
             --hiveserver_port [hive_server_port]
```

Results:
```
-- Initializing Hive Util --
Creating file /tmp/onefold_mongo/uber_events/data/1
Executing command: cat /tmp/onefold_mongo/uber_events/data/1 | json/generate-schema-mapper.py | sort | json/generate-schema-reducer.py mongodb://173.255.115.8:27017/test/uber_events_schema > /dev/null
Executing command: cat /tmp/onefold_mongo/uber_events/data/1 | json/transform-data-mapper.py mongodb://173.255.115.8:27017/test/uber_events_schema,/tmp/onefold_mongo/uber_events/data_transform/output > /dev/null
...
Executing command: hadoop fs -mkdir -p onefold_mongo/uber_events/data_transform/output/root
Executing command: hadoop fs -copyFromLocal /tmp/onefold_mongo/uber_events/data_transform/output/root/part-00000 onefold_mongo/uber_events/data_transform/output/root/
...
Executing HiveQL: create table uber_events (app_version string,stripe_customer_id int,id_oid string,event string,utm_campaign string,carrier string,time int,city string,hash_code string,device string,distinct_id string,model string,stripe_charge_id int) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
Executing HiveQL: load data inpath 'onefold_mongo/uber_events/data_transform/output/root/*' into table uber_events
...
-------------------
    RUN SUMMARY
-------------------
Extracted data with _id from 5541571c151a4b35b4000001 to 5541571c151a4b35b4000003
Extracted files are located at: /tmp/onefold_mongo/uber_events/data/1
Hive Tables: uber_events
Schema is stored in Mongo test.uber_events_schema
```

In Hive, you can see:
```
hive> add jar [install path]/hive-serdes-1.0-SNAPSHOT.jar;
hive> desc uber_events;
OK
app_version             string                  from deserializer
stripe_customer_id      int                     from deserializer
id_oid                  string                  from deserializer
event                   string                  from deserializer
utm_campaign            string                  from deserializer
carrier                 string                  from deserializer
time                    int                     from deserializer
city                    string                  from deserializer
hash_code               string                  from deserializer
device                  string                  from deserializer
distinct_id             string                  from deserializer
model                   string                  from deserializer
stripe_charge_id        int                     from deserializer
Time taken: 0.068 seconds, Fetched: 13 row(s)
hive> select * from uber_events;                                                                                                                              OK                                                                                                                                                            2.4     3748    5541571c151a4b35b4000001        set_pickup_location     Facebook_Offer  Sprint  10000   Chicago fab3915adb7adbabedf988f502bc075faaf01ab3     Samsung  14124820200     Galaxy S5       1237489                                                                                                               2.4     3749    5541571c151a4b35b4000002        set_pickup_location     Facebook_Offer  Sprint  10001   Chicago 87ffe66b3bf61d630a144e212bae1fed1f85baf7     Samsung  14124820201     Galaxy S5       1237490                                                                                                               2.4     3750    5541571c151a4b35b4000003        set_pickup_location     Facebook_Offer  Sprint  10002   Chicago 5ab6b307cbe1ac767e1afd686a01cff778069353     Samsung  14124820202     Galaxy S5       1237491                                                                                                               Time taken: 2.147 seconds, Fetched: 3 row(s)
```

In Mongo, you can see the schema saved in a collection called `uber_events_schema`:
```
> db.uber_events_schema.find();
{ "_id" : ObjectId("55415a87296e82247fdb6592"), "type" : "field", "data_type" : "string-nullable", "key" : "app_version" }
{ "_id" : ObjectId("55415a87296e82247fdb6593"), "type" : "field", "data_type" : "string-nullable", "key" : "carrier" }
{ "_id" : ObjectId("55415a87296e82247fdb6594"), "type" : "field", "data_type" : "string-nullable", "key" : "city" }
{ "_id" : ObjectId("55415a87296e82247fdb6595"), "type" : "field", "data_type" : "string-nullable", "key" : "device" }
{ "_id" : ObjectId("55415a87296e82247fdb6596"), "type" : "field", "data_type" : "string-nullable", "key" : "distinct_id" }
{ "_id" : ObjectId("55415a87296e82247fdb6597"), "type" : "field", "data_type" : "string-nullable", "key" : "event" }
{ "_id" : ObjectId("55415a87296e82247fdb6598"), "type" : "field", "data_type" : "string-nullable", "key" : "id_oid" }
{ "_id" : ObjectId("55415a87296e82247fdb6599"), "type" : "field", "data_type" : "record-nullable", "key" : "id" }
{ "_id" : ObjectId("55415a87296e82247fdb659a"), "type" : "field", "data_type" : "string-nullable", "key" : "model" }
{ "_id" : ObjectId("55415a87296e82247fdb659b"), "type" : "field", "data_type" : "integer-nullable", "key" : "stripe_charge_id" }
{ "_id" : ObjectId("55415a87296e82247fdb659c"), "type" : "field", "data_type" : "integer-nullable", "key" : "stripe_customer_id" }
{ "_id" : ObjectId("55415a87296e82247fdb659d"), "type" : "field", "data_type" : "integer-nullable", "key" : "time" }
{ "_id" : ObjectId("55415a87296e82247fdb659e"), "type" : "field", "data_type" : "string-nullable", "key" : "utm_campaign" }
{ "_id" : ObjectId("55415a872e2ecef82b7417cf"), "type" : "fragments", "fragments" : [ "root" ] }
```

Notes:
1. By default, extracted data is saved in /tmp/onefold_mongo
2. If `--use_mr` parameter is not specified, it won't use MapReduce to generate schema and transform data. This is handy if you don't have many records and/or just want to get this working quickly. In lieu of MapReduce, it will just run the mapper and reducer using command line using `cat [input] | mapper | sort | reducer` metaphor.
3. The generated HDFS file is in JSON format, so for Hive we need to include JSON Serde which is included in the build.

## Now let's add a record with new fields

In Mongo, one new record is added with





## Parameters




## Query Examples

To query for charge_id > 1237489:
```
--query '{"charge_id":{"$gt":1237489}}'
```

To query for _id > 55401a60151a4b1a4f000001:
```
--query '{"_id": {"$gt":ObjectId("55401a60151a4b1a4f000001")}}'
```


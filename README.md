hadoop-cassandra-example
========================

A simple Map/Reduce Job reading and writing from a cassandra instance running locally


Cassandra setup:

```
CREATE KEYSPACE hadoop_sample WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE input (
  name text,
  msg text,
  "count" int,
  empty text,
  PRIMARY KEY (name, msg)
)

* the count and empty column serve no purpose other than allowing for some extra logging within the map/reduce jobs

CREATE TABLE messages (
  name text PRIMARY KEY,
  msg text
)

insert into input (name, msg, count) values ('Bob', 'Says Hi', 1);
insert into input (name, msg, count) values ('Sam', 'Says Bye', 1);


input:

 name  | msg      | count | empty
-------+----------+-------+-------
 Bob   |  Says Hi |     1 |  null

messages:

 name                      | msg
---------------------------+-----------
             Bob:Says Hi   |   Says Hi

run the hadoop job with:

hadoop jar target/hadoop-cassandra-example-1.0-job.jar

```

#BiDiSync

This tool aims to serve as a start point for develop a service that automatically synchronize between Apache Cassandra and Elasticsearch. The service detect modifications from both sides and apply them on the other side, handling possible conflicts.

##Requirements

* python 2.7+
* virtualenv 1.11+
* pip 1.5+
* access to a running Apache Cassandra server with 2.0+
* access to a running Elasticsearch server 1.3+

##Running

These steps explain how to get a ready and runnind BiDiSync daemon within minutes:

1. Create a virtualenv, let's name it "env" and activate it
2. Install required python packages with pip install -r requirements.txt
3. Edit any needed configuration on conf.py, specifically Elasticsearch and Cassandra hosts/nodes options
4. Init the structures and import some data by running python tools/init.py

Now we are ready to run the daemon, if we just run python bidisync.py it will fork itself and go background. But as we are just testing, we invoke bidisync with the "-f" parameter, which makes it run on the foreground.

##How it works

The daemon check periodically for changes on both sides, comparing each record with the corresponding record on the other engine. All operations on Apache Cassandra and Elasticsearch are abstracted in what we call "drivers". Drivers encapsulates querying, executing and such, making the code easy to read and mantain (we can even implement another driver). Drivers also normalize data types for better interoperability from one engine to another, as data can be inserted from one engine to another, as a merge operation.

All changes detected are grouped in what we call "deltas". These deltas contains basic operations that turns it's associated driver onto the other synced driver. When all operations are collected, we just need to apply them with a single method.

Conflicts are resolved simply by comparing the "last_modified" field, which contains an UTC'ed datetime representing the last time the record was changed. If we have a record most recent than another one with same id on the other engine, we create an update operation and queue it on the delta for that driver.

##What else?

My knowledge of Apache Cassandra is a bit outdated now, since a read about Cassandra it was about 4 years, now we have CQL and the Thift API is being deprecated progressively. I want to make this service more efficient by using queryes that only returns the more recent changes, or even changes past toa given date. Making the service very fast and usefull.

Another improvement I would do is detecting the structures of records automatically, actually it's hardcoded on the drivers.

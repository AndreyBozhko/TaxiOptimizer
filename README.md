# TaxiOptimizer
> ***find fares and avoid competition***

This is a project I completed during the Insight Data Engineering program (New York, Summer 2018).
Visit ~~[taxioptimizer.com](http://taxioptimizer.com)~~ to see it in action.

***

This project aims at optimizing the daily routine of NYC yellow cab drivers by attempting to spread out the drivers across the city, so that they cover more neighborhoods and don't compete for passengers.

Each one of ~14,000 cabs is streaming its real-time location to **TaxiOptimizer**, and in return they are suggested up to 5 locations around them where there are higher chances to find fares. If several drivers are nearby, the service provides them with different results to reduce the competition between them.

I define the top-5 pickup spots within each neighborhood for any given 10-minute interval of the day as ~5m x 5m chunks of city streets with the most number of previous taxi rides from those spots, based on the historical data.



Pipeline
-----------------

![alt text](https://github.com/AndreyBozhko/TaxiOptimizer/blob/master/docs/pipeline.jpg "TaxiOptimizer Pipeline")

***Batch Job***: historical taxi trip data are ingested from S3 bucket into Spark, which computes top-5 pickup locations within every cell of the grid for every 10-minute interval, and saves the result into the PostgreSQL database.
> This process is automated with the help of the Airflow scheduler.

***Streaming Job***: real-time taxi location data are streamed by Kafka into Spark Streaming, where the stream is joined with the results of the batch job to produce the answers for every entry in the stream.

### Data Sources
  1. Historical: [NYC TLC Taxi Trip data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml), for period from Jan 2009 to Jun 2016 (~550 GB in total),

  2. Streaming: [MTA Bus Time historical data](http://web.mta.info/developers/MTA-Bus-Time-historical-data.html), which is treated as if the bus location data were the real-time taxi location data (streamed at ~1,000 records/s).


### Environment Setup

Install and configure [AWS CLI](https://aws.amazon.com/cli/) and [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine, and clone this repository using
`git clone https://github.com/AndreyBozhko/TaxiOptimizer`.

> AWS Tip: Add your local IP to your AWS VPC inbound rules

> Pegasus Tip: In $PEGASUS_HOME/install/download_tech, change Zookeeper version to 3.4.12, and follow the notes in docs/pegasus_setup.odt to configure Pegasus

#### CLUSTER STRUCTURE:

To reproduce my environment, 11 m4.large AWS EC2 instances are needed:

- (4 nodes) Spark Cluster - Batch
- (3 nodes) Spark Cluster - Stream
- (3 nodes) Kafka Cluster
- Flask Node

To create the clusters, put the appropriate `master.yml` and `workers.yml` files in each `cluster_setup/<clustername>` folder (following the template in `cluster_setup/dummy.yml.template`), list all the necesary software in `cluster_setup/<clustername>/install.sh`, and run the `cluster_setup/create-clusters.sh` script.

Additionally, execute `misc_scripts/copy_allnodesdns_to_masters.sh` to provide every master node with the addresses of every node from other clusters, and `misc_scripts/copy_idrsapub_to_workers.sh` to allow access to all workers via `peg ssh <clustername> <node>` command.

> After that, any node's address from cluster some-cluster-name will be saved as the environment variable SOME_CLUSTER_NAME_$i as on every master, where $i = 0, 1, 2, ...*

Lastly, run `misc_scripts/install_on_masters.sh` to clone **TaxiOptimizer** to the clusters, download necessary .jar files and install required Python packages.

##### Airflow setup

The Apache Airflow scheduler can be installed on the master node of *spark-stream-cluster*. Follow the instructions in `docs/airflow_install.txt` to launch the Airflow server.


##### PostgreSQL setup
The PostgreSQL database sits on the master node of *spark-batch-cluster*.
Follow the instructions in `docs/postgres_install.txt` to download and setup access to it.

##### Configurations
Configuration settings for Kafka, PostgreSQL, AWS S3 bucket, as well as the schemas for the data are stored in the respective files in `config/` folder.
> Replace the settings in `config/s3config.ini` with the names and paths for your S3 bucket.


## Running TaxiOptimizer

### Schedule the Batch Job
Running `airflow/schedule.sh` on the master of *spark-batch-cluster* will add the batch job to the scheduler. The batch job is set to execute every 24 hours, and it can be started and monitored from the Airflow GUI at the address `$SPARK_BATCH_CLUSTER_0:8081`.

### Start the Streaming Job
Execute `./spark-run.sh --stream` on the master of *spark-stream-cluster* (preferably after the batch job has run for the first time).

### Start streaming messages with Kafka
Execute `./kafka-run --produce` on the master of *kafka-cluster* to start streaming the real-time taxi location data.
If the topic does not exist, run `./kafka-run --create`. To describe existing topic, run `./kafka-run --describe`.
It is also possible to delete inactive topic using the option `--delete`, or view the messages in the topic with the option `--console-consume`.

### Flask
???


### Testing
The folder `test/` contains unit tests for various project components.

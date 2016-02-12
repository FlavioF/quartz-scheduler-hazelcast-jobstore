Quartz-Scheduler Hazelcast Job Store [![Build Status](https://travis-ci.org/FlavioF/quartz-scheduler-hazelcast-jobstore.svg?branch=master)](https://travis-ci.org/FlavioF/quartz-scheduler-hazelcast-jobstore) [![](https://raw.githubusercontent.com/novoda/novoda/master/assets/btn_apache_lisence.png)](LICENSE.txt)
====================================
An implementation of a Quartz Scheduler Job Store using Hazelcast distributed Maps and Sets.

This implementation is based on [Ameausoone/quartz-hazelcast-jobstore](https://github.com/Ameausoone/quartz-hazelcast-jobstore).

### About Quartz
Quartz is a richly featured, open source job scheduling library that can be integrated within virtually any Java application - from the smallest stand-alone application to the largest e-commerce system. Quartz can be used to create simple or complex schedules for executing tens, hundreds, or even tens-of-thousands of jobs; jobs whose tasks are defined as standard Java components that may execute virtually anything you may program them to do. The Quartz Scheduler includes many enterprise-class features, such as support for JTA transactions and clustering.

##### Job Stores in Quartz
JobStore's are responsible for keeping track of all the "work data" that you give to the scheduler: jobs, triggers, calendars, etc. Selecting the appropriate JobStore for your Quartz scheduler instance is an important step. Luckily, the choice should be a very easy one once you understand the differences between them. You declare which JobStore your scheduler should use (and it's configuration settings) in the properties file (or object) that you provide to the SchedulerFactory that you use to produce your scheduler instance.

[Read More](http://quartz-scheduler.org/documentation/quartz-2.x/tutorials/tutorial-lesson-09)

### About Hazelcast
Hazelcast is an in-memory open source software data grid based on Java. By having multiple nodes form a cluster, data is evenly distributed among the nodes. This allows for horizontal scaling both in terms of available storage space and processing power. Backups are also distributed in a similar fashion to other nodes, based on configuration, thereby protecting against single node failure.

[Read More](http://hazelcast.org/)

### Adding Dependency
```
 <dependency>
    <groupId>com.bikeemotion</groupId>
    <artifactId>quartz-hazelcast-jobstore</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Clustering
When using Hazelcast Job Store we relay on Hazelcast to provide a Cluster where our jobs are stored. This way we can easly have a cluster of Quartz Scheduler instance that share the same data.

### Persisting Data
Note that you can use Hazelcast MapStores to store all the data in your in-memory Maps in a datastore like Cassandra, Elasticsearch, PostgreSQL, etc (synchronously or asynchronously). Learn more about it [here](http://docs.hazelcast.org/docs/3.4/manual/html/map-persistence.html).

# Testing it
#### Pre-requisites

* JDK 8 or newer
* Maven 3.1.0 or newer

#### Clone
```
git clone https://github.com/FlavioF/quartz-scheduler-hazelcast-jobstore.git
cd quartz-scheduler-hazelcast-jobstore
```
#### Build
```
mvn clean install
```

### How to Use HazelcastJobStore with Quartz
```
// Setting Hazelcast Instance
HazelcastJobStore.setHazelcastClient(hazelcastInstance);

// Setting Hazelcast Job Store
Properties props = new Properties();
props.setProperty(StdSchedulerFactory.PROP_JOB_STORE_CLASS, HazelcastJobStore.class.getName());

StdSchedulerFactory scheduler = new StdSchedulerFactory(props).getScheduler();

// Starting Scheduler
scheduler.start();

// Scheduling job
JobDetail job = JobBuilder.newJob(jobClass).withIdentity(jobName, grouName).build();
Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerName, triggerGroup).forJob(job).startAt(new Date(startAt)).build();

scheduler.scheduleJob(job, trigger);

```

package com.bikeemotion.quartz;

import com.bikeemotion.quartz.jobstore.hazelcast.HazelcastJobStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Date;

import org.joda.time.DateTime;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;

import static org.quartz.Scheduler.DEFAULT_GROUP;

import org.quartz.ScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.spi.OperableTrigger;


public abstract class AbstractTest {

  protected HazelcastInstance hazelcastInstance;
  protected HazelcastJobStore jobStore;
  protected int buildTriggerIndex = 0;
  protected int buildJobIndex = 0;

  protected HazelcastInstance createHazelcastInstance(String clusterName) {

    Config config = new Config();
    config.getGroupConfig().setName(clusterName);
    config.getGroupConfig().setPassword("some-password");
    config.setProperty("hazelcast.logging.type", "slf4j");
    return Hazelcast.newHazelcastInstance(config);
  }

  protected HazelcastJobStore createJobStore(String name) {

    HazelcastJobStore hzJobStore = new HazelcastJobStore();
    hzJobStore.setInstanceName(name);
    return hzJobStore;
  }

  protected JobDetail buildJob() {

    return buildJob("jobName" + buildJobIndex++, DEFAULT_GROUP);
  }

  protected JobDetail buildJob(String jobName) {

    return buildJob(jobName, DEFAULT_GROUP);
  }

  protected JobDetail buildJob(String jobName, String grouName) {

    return buildJob(jobName, grouName, Job.class);
  }

  protected JobDetail buildJob(String jobName, String grouName, Class<? extends Job> jobClass) {

    JobDetail job = JobBuilder.newJob(jobClass).withIdentity(jobName, grouName).build();
    return job;
  }

  protected JobDetail storeJob(String jobName)
    throws ObjectAlreadyExistsException, JobPersistenceException {

    return storeJob(buildJob(jobName));
  }

  protected JobDetail storeJob(JobDetail jobDetail)
    throws ObjectAlreadyExistsException, JobPersistenceException {

    this.jobStore.storeJob(jobDetail, false);
    return (JobDetail) jobDetail;
  }

  protected JobDetail buildAndStoreJob()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    JobDetail buildJob = buildJob();
    this.jobStore.storeJob(buildJob, false);
    return (JobDetail) buildJob;
  }

  protected JobDetail buildAndStoreJobWithTrigger()
    throws ObjectAlreadyExistsException, JobPersistenceException {

    JobDetail buildJob = buildJob();
    this.jobStore.storeJob(buildJob, false);

    OperableTrigger trigger = buildTrigger(buildJob);
    jobStore.storeTrigger((OperableTrigger) trigger, false);

    return buildJob;
  }

  protected JobDetail retrieveJob(String jobName)
    throws JobPersistenceException {

    return this.jobStore.retrieveJob(new JobKey(jobName, DEFAULT_GROUP));
  }

  protected OperableTrigger buildTrigger(String triggerName,
                                         String triggerGroup,
                                         JobDetail job,
                                         Long startAt,
                                         Long endAt) {
   return buildTrigger(triggerName, triggerGroup, job, startAt, endAt, null);
  }

  protected OperableTrigger buildTrigger(String triggerName,
      String triggerGroup,
      JobDetail job,
      Long startAt,
      Long endAt,
      ScheduleBuilder scheduleBuilder) {

    ScheduleBuilder schedule = scheduleBuilder!=null?scheduleBuilder : SimpleScheduleBuilder.simpleSchedule();
    return (OperableTrigger) TriggerBuilder
        .newTrigger()
        .withIdentity(triggerName, triggerGroup)
        .forJob(job)
        .startAt(startAt != null ? new Date(startAt) : null)
        .endAt(endAt != null ? new Date(endAt) : null)
        .withSchedule(schedule)
        .build();
  }

  protected OperableTrigger buildTrigger(String triggerName, String triggerGroup, JobDetail job, Long startAt) {

    return buildTrigger(triggerName, triggerGroup, job, startAt, null, null);
  }

  protected OperableTrigger buildTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP, buildAndStoreJob());
  }

  protected OperableTrigger buildTrigger(String triggerName, String groupName)
    throws JobPersistenceException {

    return buildTrigger(triggerName, groupName, buildAndStoreJob());
  }

  protected OperableTrigger buildTrigger(JobDetail jobDetail) {

    return buildTrigger("triggerName" + buildTriggerIndex++, DEFAULT_GROUP, jobDetail);
  }

  protected OperableTrigger buildTrigger(String triggerName, String groupName, JobDetail jobDetail) {

    return buildTrigger(triggerName, groupName, jobDetail, DateTime.now().getMillis());
  }

  protected OperableTrigger buildAndComputeTrigger(String triggerName, String triggerGroup, JobDetail job, Long startAt) {

    return buildAndComputeTrigger(triggerName, triggerGroup, job, startAt, null);
  }

  protected OperableTrigger buildAndComputeTrigger(String triggerName,
      String triggerGroup,
      JobDetail job,
      Long startAt,
      Long endAt,
      ScheduleBuilder scheduleBuilder) {

    OperableTrigger trigger = buildTrigger(triggerName, triggerGroup, job, startAt, endAt, scheduleBuilder);
    trigger.computeFirstFireTime(null);
    return trigger;
  }

  protected OperableTrigger buildAndComputeTrigger(String triggerName,
                                                   String triggerGroup,
                                                   JobDetail job,
                                                   Long startAt,
                                                   Long endAt) {

    OperableTrigger trigger = buildTrigger(triggerName, triggerGroup, job, startAt, endAt,null);
    trigger.computeFirstFireTime(null);
    return trigger;
  }

  protected OperableTrigger buildAndStoreTrigger()
    throws ObjectAlreadyExistsException,
    JobPersistenceException {

    OperableTrigger trigger = buildTrigger();
    jobStore.storeTrigger(trigger, false);
    return trigger;
  }

  protected OperableTrigger retrieveTrigger(TriggerKey triggerKey)
    throws JobPersistenceException {

    return jobStore.retrieveTrigger(triggerKey);
  }

  protected void storeCalendar(String calName)
    throws ObjectAlreadyExistsException, JobPersistenceException {

    jobStore.storeCalendar(calName, new BaseCalendar(), false, false);
  }
}

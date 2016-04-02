package com.bikeemotion.quartz.jobstore.hazelcast;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * A no-operation {@link Job} implementation, used by unit tests.
 * 
 * @author Anton Johansson
 */
public class TestJob implements Job {

  @Override
  public void execute(JobExecutionContext context)
    throws JobExecutionException {
  }
}

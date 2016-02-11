/**
 * Copyright (C) Bikeemotion
 * 2014
 *
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authorization. All rights, including rights
 * created by patent grant or registration of a utility model or design, are
 * reserved. Modifications made to this document are restricted to authorized
 * personnel only. Technical specifications and features are binding only when
 * specifically and expressly agreed upon in a written contract.
 */
package com.bikeemotion.quartz;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MyJob implements Job, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MyJob.class);

  public static int count = 0;
  public static Queue<String> jobKeys = new LinkedList<>();
  public static Queue<String> triggerKeys = new LinkedList<>();

  @Override
  public void execute(final JobExecutionContext jobCtx)
    throws JobExecutionException {

    jobKeys.add(jobCtx.getJobDetail().getKey().getName());
    triggerKeys.add(jobCtx.getTrigger().getKey().getName());
    count++;
    LOG.info("Processing Trigger " + jobCtx.getTrigger().getKey().getName() + " " + new Date());
  }

}

package org.ameausoone;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/** An empty job for testing purpose. */
public class MyJob implements Job {
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		//
	}
}
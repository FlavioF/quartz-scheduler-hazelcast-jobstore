package org.ameausoone;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.quartz.Trigger.TriggerState.NORMAL;
import static org.quartz.Trigger.TriggerState.PAUSED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;

/**
 * @author Antoine Méausoone
 * 
 */
public class HazelcastJobStore implements JobStore {

	private final Logger log = LoggerFactory.getLogger(HazelcastJobStore.class);

	private String instanceName;

	private SchedulerSignaler schedSignaler;

	private volatile boolean schedulerRunning = false;

	private ClassLoadHelper classLoadHelper;

	private long misfireThreshold;

	private HazelcastInstance hazelcastClient;

	public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {

		this.schedSignaler = signaler;
		this.classLoadHelper = loadHelper;
		System.setProperty("hazelcast.logging.type", "slf4j");
		ClientConfig clientConfig = new ClientConfig();
		ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
		networkConfig.addAddress("127.0.0.1:5701");

		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
	}

	public void schedulerStarted() throws SchedulerException {
		log.info("Start HazelcastJobStore [" + instanceName + "]");
	}

	public long getMisfireThreshold() {
		return misfireThreshold;
	}

	/**
	 * The number of milliseconds by which a trigger must have missed its next-fire-time, in order for it to be
	 * considered "misfired" and thus have its misfire instruction applied.
	 * 
	 * @param misfireThreshold
	 *            the new misfire threshold
	 */
	public void setMisfireThreshold(long misfireThreshold) {
		if (misfireThreshold < 1) {
			throw new IllegalArgumentException("Misfire threshold must be larger than 0");
		}
		this.misfireThreshold = misfireThreshold;
	}

	public void schedulerPaused() {
		schedulerRunning = false;

	}

	public void schedulerResumed() {
		schedulerRunning = true;

	}

	public void shutdown() {
		hazelcastClient.shutdown();
	}

	public boolean supportsPersistence() {
		return false;
	}

	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		// TODO Auto-generated method stub
		// C/C from RAMJobStore
		return 5;
	}

	public boolean isClustered() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<JobKey, JobDetail> getJobMap() {
		return hazelcastClient.getMap(instanceName + '-' + "JobMap"

		);
	}

	/**
	 * JobKeys by group name return a Multimap which map a groupName to a List of JobKeys
	 */
	private MultiMap<String, JobKey> getJobKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + '-' + "JobByGroupMap");
	}

	/**
	 * @return Map which contains Trigger by TriggerKey.
	 */
	private IMap<TriggerKey, TriggerWrapper> getTriggerByKeyMap() {
		return hazelcastClient.getMap(instanceName + '-' + "TriggerMap");
	}

	/**
	 * TriggerKeys by Trigger group name
	 */
	private MultiMap<String, TriggerKey> getTriggerKeyByGroupMap() {
		return hazelcastClient.getMultiMap(instanceName + '-' + "TriggerByGroupMap");
	}

	/**
	 * @return Map which contains Trigger by TriggerKey.
	 */
	// private IMap<TriggerKey, TriggerState> getTriggerStateMap() {
	// return hazelcastClient.getMap(instanceName + '-' + "TriggerStateMap"
	//
	// );
	// }

	private ISet<String> getHZPausedTriggerGroups() {
		return hazelcastClient.getSet(instanceName + '-' + "PausedTriggerGroupSet");
	}

	private ISet<String> getPausedJobGroups() {
		return hazelcastClient.getSet(instanceName + '-' + "PausedJobGroupSet");
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		storeJob(newJob, false);
		storeTrigger(newTrigger, false);

	}

	public void storeJob(JobDetail job, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		JobDetail newJob = (JobDetail) job.clone();
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		JobKey newJobKey = newJob.getKey();
		boolean containsKey = jobMap.containsKey(newJobKey);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException(newJob);
		}
		jobMap.lock(newJobKey);
		try {
			jobMap.put(newJobKey, newJob);
			getJobKeyByGroupMap().put(newJobKey.getGroup(), newJobKey);
		} finally {
			jobMap.unlock(newJobKey);
		}
	}

	public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
			throws ObjectAlreadyExistsException, JobPersistenceException {

		if (!replace) {
			for (Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
				JobDetail jobDetail = e.getKey();
				if (checkExists(jobDetail.getKey()))
					throw new ObjectAlreadyExistsException(jobDetail);
				for (Trigger trigger : e.getValue()) {
					if (checkExists(trigger.getKey()))
						throw new ObjectAlreadyExistsException(trigger);
				}
			}
		}
		// do bulk add...
		for (Entry<JobDetail, Set<? extends Trigger>> e : triggersAndJobs.entrySet()) {
			storeJob(e.getKey(), true);
			for (Trigger trigger : e.getValue()) {
				storeTrigger((OperableTrigger) trigger, true);
			}
		}

	}

	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (found) {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				removeTrigger(trigger.getKey());
			}
			getJobKeyByGroupMap().remove(jobKey.getGroup(), jobKey);
			return jobMap.remove(jobKey) != null;
		}
		return false;

	}

	public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
		boolean jobExists = false;
		for (JobKey jobKey : jobKeys) {
			boolean removeJob = removeJob(jobKey);
			if (removeJob) {
				jobExists = true;
			}
		}
		return jobExists;
	}

	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		if (jobKey == null)
			return null;
		if (jobMap.containsKey(jobKey)) {
			return jobMap.get(jobKey);
		}
		return null;
	}

	public void storeTrigger(OperableTrigger trigger, boolean replaceExisting) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		OperableTrigger newTrigger = (OperableTrigger) trigger.clone();
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		TriggerKey triggerKey = newTrigger.getKey();
		triggerMap.lock(triggerKey);
		try {
			boolean containsKey = triggerMap.containsKey(triggerKey);
			if (containsKey && !replaceExisting)
				throw new ObjectAlreadyExistsException(newTrigger);

			if (retrieveJob(newTrigger.getJobKey()) == null)
				throw new JobPersistenceException("The job (" + newTrigger.getJobKey()
						+ ") referenced by the trigger does not exist.");

			boolean shouldBePausedByJobGroup = getPausedJobGroups().contains(newTrigger.getJobKey().getGroup());
			boolean shouldBePausedByTriggerGroup = getHZPausedTriggerGroups().contains(triggerKey.getGroup());
			boolean shouldBePaused = shouldBePausedByJobGroup || shouldBePausedByTriggerGroup;
			TriggerState state = shouldBePaused ? PAUSED : NORMAL;

			TriggerWrapper tw = new TriggerWrapper(newTrigger);
			tw.state = state;
			triggerMap.put(triggerKey, tw);

			getTriggerKeyByGroupMap().put(triggerKey.getGroup(), triggerKey);
		} finally {
			triggerMap.unlock(triggerKey);
		}
	}

	public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// TODO implement remove orphanedJob
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		boolean found = triggerMap.containsKey(triggerKey);
		if (found) {
			triggerMap.lock(triggerKey);
			try {
				getTriggerKeyByGroupMap().remove(triggerKey.getGroup(), triggerKey);
				triggerMap.remove(triggerKey);
			} finally {
				triggerMap.unlock(triggerKey);
			}
		}
		return found;
	}

	public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
		boolean triggerExists = false;
		for (TriggerKey triggerKey : triggerKeys) {
			boolean removetrigger = removeTrigger(triggerKey);
			if (removetrigger) {
				triggerExists = true;
			}
		}
		return triggerExists;
	}

	public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
		newTrigger.setKey(triggerKey);
		boolean checkExists = checkExists(triggerKey);
		storeTrigger(newTrigger, true);
		return checkExists;
	}

	public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		TriggerWrapper triggerWrapper = triggerMap.get(triggerKey);
		if (triggerWrapper == null)
			return null;
		return (OperableTrigger) triggerWrapper.getTrigger();
	}

	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		return jobMap.containsKey(jobKey);
	}

	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		return triggerMap.containsKey(triggerKey);
	}

	public void clearAllSchedulingData() throws JobPersistenceException {
		for (TriggerKey triggerKey : getTriggerByKeyMap().keySet()) {
			removeTrigger(triggerKey);
		}

		ISet<String> hzPausedTriggerGroups = getHZPausedTriggerGroups();
		for (String group : hzPausedTriggerGroups) {
			hzPausedTriggerGroups.remove(group);
		}

		IMap<JobKey, JobDetail> jobMap = getJobMap();
		for (JobKey jobKey : jobMap.keySet()) {
			removeJob(jobKey);
		}

		ISet<String> pausedJobGroups = getPausedJobGroups();
		for (String pausedJobGroup : pausedJobGroups) {
			pausedJobGroups.remove(pausedJobGroup);
		}

		IMap<String, Calendar> calendarMap = getCalendarMap();
		for (String calName : calendarMap.keySet()) {
			removeCalendar(calName);
		}
	}

	/**
	 * @return Map which contains Jobs.
	 */
	private IMap<String, Calendar> getCalendarMap() {
		IMap<String, Calendar> calendarMap = hazelcastClient.getMap(instanceName + "CalendarMap");
		return calendarMap;
	}

	public void storeCalendar(String calName, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		IMap<String, Calendar> calendarMap = getCalendarMap();
		boolean containsKey = calendarMap.containsKey(calName);
		if (containsKey && !replaceExisting) {
			throw new ObjectAlreadyExistsException("Calendar with name '" + calName + "' already exists.");
		} else {
			calendarMap.put(calName, calendar);
		}

	}

	public boolean removeCalendar(String calName) throws JobPersistenceException {
		// TODO implement
		// "If removal of the Calendar would result in Triggers pointing to non-existent calendars, then a JobPersistenceException will be thrown."
		return getCalendarMap().remove(calName) != null;
	}

	public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
		IMap<String, Calendar> calendarMap = getCalendarMap();
		return calendarMap.get(calName);
	}

	public int getNumberOfJobs() throws JobPersistenceException {
		return getJobMap().size();
	}

	public int getNumberOfTriggers() throws JobPersistenceException {
		return getTriggerByKeyMap().size();
	}

	public int getNumberOfCalendars() throws JobPersistenceException {
		return getCalendarMap().size();
	}

	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
		Set<JobKey> outList = null;
		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		String groupNameCompareValue = matcher.getCompareToValue();
		switch (operator) {
		case EQUALS:
			Collection<JobKey> jobKeys = getJobKeyByGroupMap().get(groupNameCompareValue);
			if (jobKeys != null) {
				outList = new HashSet<JobKey>();

				for (JobKey jobKey : jobKeys) {

					if (jobKey != null) {
						outList.add(jobKey);
					}
				}
			}
			break;
		default:
			for (String groupName : getJobKeyByGroupMap().keySet()) {
				if (operator.evaluate(groupName, groupNameCompareValue)) {
					if (outList == null) {
						outList = new HashSet<JobKey>();
					}
					for (JobKey jobKey : getJobKeyByGroupMap().get(groupName)) {
						if (jobKey != null) {
							outList.add(jobKey);
						}
					}
				}
			}
		}

		return outList == null ? java.util.Collections.<JobKey> emptySet() : outList;
	}

	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		Set<TriggerKey> outList = null;
		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		String groupNameCompareValue = matcher.getCompareToValue();
		switch (operator) {
		case EQUALS:
			Collection<TriggerKey> triggerKeys = getTriggerKeyByGroupMap().get(groupNameCompareValue);
			if (triggerKeys != null) {
				outList = newHashSet();

				for (TriggerKey triggerKey : triggerKeys) {

					if (triggerKey != null) {
						outList.add(triggerKey);
					}
				}
			}
			break;
		default:
			for (String groupName : getTriggerKeyByGroupMap().keySet()) {
				if (operator.evaluate(groupName, groupNameCompareValue)) {
					if (outList == null) {
						outList = newHashSet();
					}
					for (TriggerKey triggerKey : getTriggerKeyByGroupMap().get(groupName)) {
						if (triggerKey != null) {
							outList.add(triggerKey);
						}
					}
				}
			}
		}
		return outList == null ? java.util.Collections.<TriggerKey> emptySet() : outList;
	}

	public List<String> getJobGroupNames() throws JobPersistenceException {
		return newArrayList(getJobKeyByGroupMap().keySet());
	}

	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		Set<TriggerKey> triggerKeys = getTriggerByKeyMap().keySet();
		Set<String> groupNames = newHashSet();
		for (TriggerKey triggerKey : triggerKeys) {
			groupNames.add(triggerKey.getGroup());
		}
		return newArrayList(groupNames);
	}

	public List<String> getCalendarNames() throws JobPersistenceException {
		return newArrayList(getCalendarMap().keySet());
	}

	public List<OperableTrigger> getTriggersForJob(final JobKey jobKey) throws JobPersistenceException {
		// TODO IMap.values(Predicate predicate) is certainly more efficient, but didn't find how to fix : "Caused by:
		// java.io.NotSerializableException: org.ameausoone.HazelcastJobStore."
		if (jobKey == null)
			return Collections.emptyList();
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		Collection<TriggerWrapper> values = triggerMap.values();
		List<OperableTrigger> outList = Lists.newArrayList();
		for (TriggerWrapper tw : values) {
			if (jobKey.equals(tw.getTrigger().getJobKey()))
				outList.add((OperableTrigger) tw.getTrigger());
		}
		return outList;
	}

	public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		// IMap<TriggerKey, TriggerState> triggerStateMap = getTriggerStateMap();
		IMap<TriggerKey, TriggerWrapper> triggerByKeyMap = getTriggerByKeyMap();

		triggerByKeyMap.lock(triggerKey);
		try {
			TriggerWrapper tw = triggerByKeyMap.get(triggerKey);
			tw.state = TriggerState.PAUSED;
			triggerByKeyMap.put(triggerKey, tw);
		} finally {
			triggerByKeyMap.unlock(triggerKey);
		}
	}

	public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		triggerMap.lock(triggerKey);
		TriggerState state = null;
		try {
			TriggerWrapper tw = triggerMap.get(triggerKey);
			if (tw != null)
				state = tw.state;
		} finally {
			triggerMap.unlock(triggerKey);
		}
		return state;
	}

	public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerMap = getTriggerByKeyMap();
		triggerMap.lock(triggerKey);
		try {
			TriggerWrapper tw = triggerMap.get(triggerKey);
			tw.state = NORMAL;
			triggerMap.put(triggerKey, tw);
		} finally {
			triggerMap.unlock(triggerKey);
		}
	}

	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		List<String> pausedGroups = new LinkedList<String>();

		StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
		switch (operator) {
		case EQUALS:
			if (getHZPausedTriggerGroups().add(matcher.getCompareToValue())) {
				pausedGroups.add(matcher.getCompareToValue());
			}
			break;
		default:
			for (String group : getTriggerKeyByGroupMap().keySet()) {
				if (operator.evaluate(group, matcher.getCompareToValue())) {
					if (getHZPausedTriggerGroups().add(matcher.getCompareToValue())) {
						pausedGroups.add(group);
					}
				}
			}
		}

		for (String pausedGroup : pausedGroups) {
			Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.triggerGroupEquals(pausedGroup));
			for (TriggerKey key : keys) {
				pauseTrigger(key);
			}
		}
		return pausedGroups;
	}

	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
		Set<String> resumeGroups = new HashSet<String>();
		Set<TriggerKey> keys = getTriggerKeys(matcher);
		ISet<String> pausedTriggerGroups = getHZPausedTriggerGroups();

		for (TriggerKey triggerKey : keys) {
			resumeGroups.add(triggerKey.getGroup());
			TriggerWrapper tw = getTriggerByKeyMap().get(triggerKey);
			OperableTrigger trigger = tw.getTrigger();
			String jobGroup = trigger.getJobKey().getGroup();
			if (getPausedJobGroups().contains(jobGroup)) {
				continue;
			}
			resumeTrigger(triggerKey);
		}
		for (String group : resumeGroups) {
			pausedTriggerGroups.remove(group);
		}
		return new ArrayList<String>(resumeGroups);
	}

	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (!found)
			return;
		jobMap.lock(jobKey);
		try {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				pauseTrigger(trigger.getKey());
			}
		} finally {
			jobMap.unlock(jobKey);
		}
	}

	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		IMap<JobKey, JobDetail> jobMap = getJobMap();
		boolean found = jobMap.containsKey(jobKey);
		if (!found)
			return;
		jobMap.lock(jobKey);
		try {
			List<OperableTrigger> triggersForJob = getTriggersForJob(jobKey);
			for (OperableTrigger trigger : triggersForJob) {
				resumeTrigger(trigger.getKey());
			}
		} finally {
			jobMap.unlock(jobKey);
		}

	}

	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
		List<String> pausedGroups = new LinkedList<String>();
		ISet<String> pausedJobGroups = getPausedJobGroups();
		StringMatcher.StringOperatorName operator = groupMatcher.getCompareWithOperator();
		MultiMap<String, JobKey> jobKeyByGroupMap = getJobKeyByGroupMap();
		switch (operator) {
		case EQUALS:
			if (pausedJobGroups.add(groupMatcher.getCompareToValue())) {
				pausedGroups.add(groupMatcher.getCompareToValue());
			}
			break;
		default:
			for (String jobGroup : jobKeyByGroupMap.keySet()) {
				if (operator.evaluate(jobGroup, groupMatcher.getCompareToValue())) {
					if (pausedJobGroups.add(jobGroup)) {
						pausedGroups.add(jobGroup);
					}
				}
			}
		}

		for (String groupName : pausedGroups) {
			for (JobKey jobKey : getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
				pauseJob(jobKey);
			}
		}
		return pausedGroups;
	}

	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
		Set<String> resumeGroups = new HashSet<String>();

		Set<JobKey> jobKeys = getJobKeys(matcher);
		ISet<String> pausedJobGroups = getPausedJobGroups();

		for (JobKey jobKey : jobKeys) {
			resumeGroups.add(jobKey.getGroup());
			resumeJob(jobKey);
		}
		for (String group : resumeGroups) {
			pausedJobGroups.remove(group);
		}
		return new ArrayList<String>(resumeGroups);
	}

	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		return new HashSet<String>(getHZPausedTriggerGroups());
	}

	public void pauseAll() throws JobPersistenceException {
		List<String> triggerGroupNames = getTriggerGroupNames();
		for (String triggerGroup : triggerGroupNames) {
			pauseTriggers(GroupMatcher.triggerGroupEquals(triggerGroup));
		}
	}

	public void resumeAll() throws JobPersistenceException {
		List<String> triggerGroupNames = getTriggerGroupNames();
		for (String triggerGroup : triggerGroupNames) {
			resumeTriggers(GroupMatcher.triggerGroupEquals(triggerGroup));
		}
	}

	public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
			throws JobPersistenceException {
		IMap<TriggerKey, TriggerWrapper> triggerByKeyMap = getTriggerByKeyMap();
		Trigger trigger;
		return null;
	}

	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		// TODO Auto-generated method stub

	}

	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail,
			CompletedExecutionInstruction triggerInstCode) {
		// TODO Auto-generated method stub

	}

	public void setInstanceId(String schedInstId) {
		// TODO Auto-generated method stub

	}

	public void setInstanceName(String name) {
		this.instanceName = name;
	}

	public void setThreadPoolSize(int poolSize) {
		// TODO Auto-generated method stub

	}

}

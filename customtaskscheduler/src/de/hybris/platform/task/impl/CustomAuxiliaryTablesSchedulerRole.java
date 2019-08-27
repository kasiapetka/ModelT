
package de.hybris.platform.task.impl;

import de.hybris.platform.core.model.type.ComposedTypeModel;
import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.task.TaskConditionModel;
import de.hybris.platform.task.TaskModel;
import de.hybris.platform.task.impl.AuxiliaryTablesBasedTaskProvider.Params;
import de.hybris.platform.task.impl.gateways.SchedulerState;
import de.hybris.platform.task.impl.gateways.TasksQueueGateway;
import de.hybris.platform.task.impl.gateways.WorkerStateGateway;
import de.hybris.platform.task.impl.gateways.WorkerStateGateway.WorkerRange;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Suppliers;


public class CustomAuxiliaryTablesSchedulerRole extends AuxiliaryTablesSchedulerRole
{

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomAuxiliaryTablesSchedulerRole.class);

	private Instant timestamp = Instant.now();

	private AuxiliaryTablesGatewayFactory gatewayFactory;

	private MetricRegistry metricRegistry;

	private TypeService typeService;
	private Timer activateWorkersTimer;
	private Timer deactivateWorkersTimer;
	private Timer lockTimer;
	private Timer schedulerTimer;
	private Timer copyTasksTimer;
	private Timer countTasksTimer;

	private final Supplier<String> conditionsQuerySupplier = Suppliers.memoize(
			() -> MessageFormat.format("SELECT PK, hjmpTS FROM {0} WHERE {1} < ? AND {2} IS NULL AND ({3} = 0 OR {3} IS NULL)",
					getTableName(TaskConditionModel._TYPECODE),
					getColumnName(TaskConditionModel._TYPECODE, TaskConditionModel.EXPIRATIONTIMEMILLIS),
					getColumnName(TaskConditionModel._TYPECODE, TaskConditionModel.TASK),
					getColumnName(TaskConditionModel._TYPECODE, TaskConditionModel.FULFILLED)));

	@Override
	public Collection<TasksProvider.VersionPK> tryToPerformSchedulerJob(final RuntimeConfigHolder runtimeConfigHolder,
	                                                                    final TaskEngineParameters taskEngineParameters,
	                                                                    final int maxItemsToSchedule)
	{

		final int nodeId = taskEngineParameters.getClusterNodeID();
		final Duration lockingTryMinInterval = runtimeConfigHolder.getProperty(Params.SCHEDULER_INTERVAL);
		final boolean isScheduler = lockSchedulerRole(nodeId, lockingTryMinInterval);

		if (isScheduler)
		{
			LOGGER.debug("{} (scheduler): lock acquired - performing scheduler's jobs", nodeId);

			final Timer.Context timer = this.schedulerTimer.time();

			final List<WorkerStateGateway.WorkerState> workers = gatewayFactory.getWorkerStateGateway().getWorkers();

			final List<WorkerStateGateway.WorkerState> activeWorkers = activateWorkers(workers, nodeId,
					runtimeConfigHolder.getProperty(
							Params.WORKER_ACTIVATION_INTERVAL),
					runtimeConfigHolder);
			deactivateWorkers(workers, nodeId, runtimeConfigHolder.getProperty(Params.WORKER_DEACTIVATION_INTERVAL));
			cleanLockedTasks(runtimeConfigHolder.getProperty(Params.WORKER_REMOVAL_INTERVAL));

			gatewayFactory.getTasksQueueGateway()
			              .clean(runtimeConfigHolder.getProperty(Params.SCHEDULER_CLEAN_QUEUE_OLD_TASKS_THRESHOLD));

			final Collection<TasksProvider.VersionPK> conditions = copyTasksToAuxiliaryTable(maxItemsToSchedule, activeWorkers,
					nodeId, runtimeConfigHolder);

			final long time = timer.stop();
			if (LOGGER.isDebugEnabled())
			{
				LOGGER.debug("{} (scheduler): done performing scheduler's jobs, it took {}", nodeId,
						DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(time)));
			}
			return conditions;
		}

		return Collections.emptyList();
	}

	private Collection<TasksProvider.VersionPK> copyTasksToAuxiliaryTable(final int maxItemsToSchedule,
	                                                                      final List<WorkerStateGateway.WorkerState> activeWorkers,
	                                                                      final int nodeId,
	                                                                      final RuntimeConfigHolder configHolder)
	{
		final List<WorkerStateGateway.WorkerState> workersWithNoTasksToProcess = getWorkersWithLowTasksCount(activeWorkers,
				nodeId,
				maxItemsToSchedule,
				configHolder);
		if (workersWithNoTasksToProcess.isEmpty())
		{
			return Collections.emptyList();
		}

		final int rangeStart = configHolder.getProperty(Params.TASKS_RANGE_START);
		final int rangeEnd = configHolder.getProperty(Params.TASKS_RANGE_END);

		final String tasksQuery = getTasksQuery(rangeStart, rangeEnd);

		final String conditionsQuery = getConditionsQuery();

		final String expiredTasksQuery = getExpiredTasksQuery(rangeStart, rangeEnd);

		final Timer.Context timer = this.copyTasksTimer.time();
		long tasksCount = 0;

		final List<TasksProvider.VersionPK> conditions = new ArrayList<>();
		final TasksQueueGateway tasksQueueGateway = gatewayFactory.getTasksQueueGateway();

		try
		{
			final Instant now = Instant.now();
			conditions.addAll(tasksQueueGateway.getConditionsToSchedule(conditionsQuery, now));
			tasksCount = tasksQueueGateway.addTasks(tasksQuery, expiredTasksQuery, now, rangeStart, rangeEnd);
		}
		catch (final Exception e)
		{
			LOGGER.error("Error while retrieving tasks to schedule", e);
		}

		final long time = timer.stop();
		if (LOGGER.isDebugEnabled())
		{
			LOGGER.debug("{} (scheduler): copied {} tasks to queue, found {} conditions, it took {}", nodeId, tasksCount,
					conditions.size(), DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(time)));
		}
		return conditions;
	}

	private static boolean isWorkerWithLowTasksCount(final int maxItemsToSchedule, final long tasksInQueueForWorker,
	                                                 final RuntimeConfigHolder configHolder)
	{
		return tasksInQueueForWorker < maxItemsToSchedule
				* configHolder.getProperty(Params.WORKER_LOW_TASKS_COUNT_THRESHOLD_MULTIPLIER);
	}

	private boolean lockSchedulerRole(final int nodeId, final Duration lockingTryMinInterval)
	{

		final Timer.Context timer = this.lockTimer.time();
		boolean lockAcquired;

		final Instant now = Instant.now();
		final Duration duration = Duration.between(timestamp, now);

		if (duration.toMillis() < lockingTryMinInterval.toMillis())
		{
			return false;
		}

		LOGGER.debug("{}: trying to acquire lock to be a scheduler", nodeId);
		final Optional<SchedulerState> result = gatewayFactory.getSchedulerStateGateway().getSchedulerTimestamp();

		final boolean recreateTables;

		if (result.isPresent())
		{
			recreateTables = result.get().getVersion() != AuxiliaryTablesBasedTaskProvider.VERSION;

			final Instant schedulerTimestamp = result.get().getTimestamp().toInstant();
			final Duration durationDb = Duration.between(schedulerTimestamp, now);
			if (LOGGER.isDebugEnabled())
			{
				LOGGER.debug("{}: got scheduler timestamp {}, now is {}, duration is {}{}", nodeId, schedulerTimestamp, now,
						durationDb.isNegative() ? "-" : "",
						DurationFormatUtils.formatDurationHMS(durationDb.abs().toMillis()));
			}

			if (durationDb.toMillis() > lockingTryMinInterval.toMillis())
			{
				lockAcquired = gatewayFactory.getSchedulerStateGateway().updateSchedulerRow(now, schedulerTimestamp);
				timestamp = now;
			}
			else
			{
				lockAcquired = false;
			}
		}
		else
		{
			recreateTables = false;
			lockAcquired = gatewayFactory.getSchedulerStateGateway().insertSchedulerRow(now,
					AuxiliaryTablesBasedTaskProvider.VERSION);
		}

		if (lockAcquired && recreateTables)
		{
			recreateAuxiliaryTables(nodeId);
			lockAcquired = gatewayFactory.getSchedulerStateGateway().insertSchedulerRow(now,
					AuxiliaryTablesBasedTaskProvider.VERSION);
		}

		timer.stop();
		return lockAcquired;
	}

	private void recreateAuxiliaryTables(final int nodeId)
	{
		final Instant start = Instant.now();
		LOGGER.debug("{} (scheduler): recreating tables", nodeId);

		try
		{
			gatewayFactory.getTasksQueueGateway().dropTable();
		}
		catch (final Exception e)
		{
			LOGGER.debug("error while dropping tasks queue table", e);
		}

		try
		{
			gatewayFactory.getWorkerStateGateway().dropTable();
		}
		catch (final Exception e)
		{
			LOGGER.debug("error while dropping workers state table", e);
		}

		try
		{
			gatewayFactory.getSchedulerStateGateway().dropTable();
		}
		catch (final Exception e)
		{
			LOGGER.debug("error while dropping scheduler state table", e);
		}

		gatewayFactory.getTasksQueueGateway().createTable();
		gatewayFactory.getWorkerStateGateway().createTable();
		gatewayFactory.getSchedulerStateGateway().createTable();

		LOGGER.debug("{} (scheduler): finished recreating tables, it took {}", nodeId, Duration.between(start, Instant.now()));
	}

	private List<WorkerStateGateway.WorkerState> activateWorkers(final List<WorkerStateGateway.WorkerState> workers,
	                                                             final int nodeId, final Duration workerHealthCheckInterval,
	                                                             final RuntimeConfigHolder configHolder)
	{
		final Timer.Context timer = this.activateWorkersTimer.time();
		final List<WorkerStateGateway.WorkerState> activeWorkers = workers.stream()
		                                                                  .filter(resultRow -> resultRow.getTimeFromHealthCheck()
		                                                                                                .toMillis() <= workerHealthCheckInterval
				                                                                  .toMillis())
		                                                                  .collect(Collectors.toList());

		final int rangeStart = configHolder.getProperty(Params.TASKS_RANGE_START);
		final int rangeEnd = configHolder.getProperty(Params.TASKS_RANGE_END);

		final Map<Integer, WorkerRange> ranges = calculateRanges(activeWorkers, rangeStart, rangeEnd);

		if (!ranges.isEmpty())
		{
			gatewayFactory.getWorkerStateGateway().updateWorkersRanges(ranges);
		}

		final long time = timer.stop();
		if (LOGGER.isDebugEnabled())
		{
			LOGGER.debug("{} (scheduler): activated {} workers (ids: {}), it took {}", nodeId, activeWorkers.size(),
					activeWorkers.stream().map(WorkerStateGateway.WorkerState::getNodeId).collect(Collectors.toList()),
					DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(time)));
		}

		return activeWorkers;

	}

	private static Map<Integer, WorkerRange> calculateRanges(final List<WorkerStateGateway.WorkerState> activeWorkers,
         final int rangeStart, final int rangeEnd)
   {
		int left = rangeEnd - rangeStart;
		int currentBucketRangeStart = rangeStart;

      final Map<Integer, WorkerRange> ranges = new HashMap<>();

      final List<WorkerStateGateway.WorkerState> activeWorkersNotExclusive = activeWorkers.stream().filter(
      workerState -> !workerState.isExclusiveMode()).collect(Collectors.toList());

      ranges.putAll(
      activeWorkers.stream().filter(WorkerStateGateway.WorkerState::isExclusiveMode).collect(Collectors.toMap(
      WorkerStateGateway.WorkerState::getNodeId, w -> new WorkerRange(rangeStart, rangeEnd))));

      for (int i = 0; i < activeWorkersNotExclusive.size(); i++)
      {
         final int nodeId = activeWorkersNotExclusive.get(i).getNodeId();
         final int j = activeWorkersNotExclusive.size() - i;

         final int bucketSize = left / j;
         final int currentBucketRangeEnd = currentBucketRangeStart + bucketSize;

         ranges.put(nodeId, new WorkerRange(currentBucketRangeStart, currentBucketRangeEnd));

         currentBucketRangeStart = currentBucketRangeEnd;
         left -= bucketSize;
      }
      return ranges;
   }

	private void deactivateWorkers(final List<WorkerStateGateway.WorkerState> workers, final int nodeId,
	                               final Duration workerHealthCheckInterval)
	{

		final Timer.Context timer = this.deactivateWorkersTimer.time();
		final List<Integer> invalidWorkerIds = workers.stream()
		                                              .filter(workerState -> workerState.getTimeFromHealthCheck()
		                                                                                .toMillis() > workerHealthCheckInterval.toMillis())
		                                              .map(WorkerStateGateway.WorkerState::getNodeId)
		                                              .collect(Collectors.toList());

		if (!invalidWorkerIds.isEmpty())
		{
			gatewayFactory.getWorkerStateGateway().deactivateWorkers(invalidWorkerIds);
		}

		final long time = timer.stop();
		if (LOGGER.isDebugEnabled())
		{
			LOGGER.debug("{} (scheduler): deactivated {} workers, it took {}", nodeId, invalidWorkerIds.size(),
					DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(time)));
		}

	}

	private void cleanLockedTasks(final Duration workerHealthCheckInterval)
	{
		final Map<Integer, Duration> r = gatewayFactory.getWorkerStateGateway().getWorkersHealthChecks();
		final List<Integer> invalidWorkerIds = r.entrySet()
		                                        .stream()
		                                        .filter(e -> e.getValue().toMillis() > workerHealthCheckInterval.toMillis())
		                                        .map(Map.Entry::getKey)
		                                        .collect(Collectors.toList());

		if (!invalidWorkerIds.isEmpty())
		{
			gatewayFactory.getWorkerStateGateway().deleteWorkers(invalidWorkerIds);
			gatewayFactory.getTasksQueueGateway().unlockTasksForWorkers(invalidWorkerIds);
		}
	}

	private List<WorkerStateGateway.WorkerState> getWorkersWithLowTasksCount(
			final List<WorkerStateGateway.WorkerState> activeWorkers, final int nodeId, final int maxItemsToSchedule,
			final RuntimeConfigHolder configHolder)
	{
		final Timer.Context timer = countTasksTimer.time();
		final Map<WorkerStateGateway.WorkerState, Long> result = activeWorkers.stream().collect(HashMap::new,
				(map, workerState) -> map.put(
						workerState, 0L),
				Map::putAll);

		final List<TasksQueueGateway.TasksCountResult> numberOfScheduledTasks = gatewayFactory.getTasksQueueGateway()
		                                                                                      .getTasksCount();

		for (final TasksQueueGateway.TasksCountResult t : numberOfScheduledTasks)
		{
			final Integer workerNodeId = t.getNodeId();
			final String nodeGroup = t.getNodeGroups();
			final Long count = t.getCount();

			final Stream<WorkerStateGateway.WorkerState> workersToCount;
			if (nodeGroup == null && workerNodeId == null)
			{
				//special case - only workers not in exclusive mode
				workersToCount = activeWorkers.stream().filter(workerState -> !workerState.isExclusiveMode());
			}
			else
			{
				workersToCount = activeWorkers.stream().filter(workerState -> {
					if (workerState.isExclusiveMode())
					{
						return Objects.equals(workerNodeId, workerState.getNodeId())
								|| (workerNodeId == null && workerState.getNodeGroups().contains(nodeGroup));
					}

					return (workerNodeId == null || Objects.equals(workerNodeId, workerState.getNodeId()))
							&& (nodeGroup == null || workerState.getNodeGroups().contains(nodeGroup));

				});
			}

			workersToCount.forEach(
					workerState -> result.compute(workerState, (w, c) -> ObjectUtils.defaultIfNull(c, 0L) + count));
		}

		final long time = timer.stop();
		if (LOGGER.isDebugEnabled())
		{
			final String statsString = result.entrySet().stream()
			                                 .map(workerState -> MessageFormat.format(
					                                 "'{'id: {0}; exclusiveMode: {1}; nodeGroups: {2}; count: {3}'}'",
					                                 workerState.getKey().getNodeId(), workerState.getKey().isExclusiveMode(),
					                                 workerState.getKey().getNodeGroups(), workerState.getValue()))
			                                 .collect(Collectors.joining(",", "[", "]"));
			LOGGER.debug("{} (scheduler): calculated tasks count: {}, it took {}", nodeId, statsString,
					DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(time)));
		}

		return result.entrySet().stream().filter(
				workerStateLongEntry -> isWorkerWithLowTasksCount(maxItemsToSchedule, workerStateLongEntry.getValue(),
						configHolder))
		             .map(Map.Entry::getKey).collect(Collectors.toList());
	}

	@Override
	protected String getConditionsQuery()
	{
		return conditionsQuerySupplier.get();
	}

	@Override
	protected String getExpiredTasksQuery(final int rangeStart, final int rangeEnd)
	{
		final TasksQueueGateway tasksQueueGateway = gatewayFactory.getTasksQueueGateway();
		final String randomRangeSQLExpression = tasksQueueGateway.getRangeSQLExpression(rangeStart, rangeEnd);
		final String nodeGroupExpression = tasksQueueGateway.defaultIfNull("p_nodeGroup", tasksQueueGateway.getEmptyGroupValue());
		final String nodeIdExpression = tasksQueueGateway.defaultIfNull("p_nodeId", -1);

		return MessageFormat.format(
				"SELECT PK, {0} AS rangeCol, {5} AS nodeIdCol, {6} AS nodeGroupCol, {1}/1000/60 AS execTimeCol FROM {2} WHERE {3} = 0 AND {1} <= ? AND {4} = -1",
				randomRangeSQLExpression, getColumnName(TaskModel._TYPECODE, TaskModel.EXPIRATIONTIMEMILLIS),
				getTableName(TaskModel._TYPECODE), getColumnName(TaskModel._TYPECODE, TaskModel.FAILED),
				getColumnName(TaskModel._TYPECODE, TaskModel.RUNNINGONCLUSTERNODE), nodeIdExpression, nodeGroupExpression);

	}

	@Override
	protected String getTasksQuery(final int rangeStart, final int rangeEnd)
	{
		final TasksQueueGateway tasksQueueGateway = gatewayFactory.getTasksQueueGateway();
		final String randomRangeSQLExpression = tasksQueueGateway.getRangeSQLExpression(rangeStart, rangeEnd);
		final String nodeGroupExpression = tasksQueueGateway.defaultIfNull("p_nodeGroup", tasksQueueGateway.getEmptyGroupValue());
		final String nodeIdExpression = tasksQueueGateway.defaultIfNull("p_nodeId", -1);

		final String conditionSubQuery = MessageFormat.format("SELECT p_task FROM {0} WHERE {1} =t.PK AND {2} = 0",
				getTableName(TaskConditionModel._TYPECODE),
				getColumnName(TaskConditionModel._TYPECODE,
						TaskConditionModel.TASK),
				getColumnName(TaskConditionModel._TYPECODE,
						TaskConditionModel.FULFILLED));

		return MessageFormat.format(
				"SELECT PK, {0} AS rangeCol, {6} AS nodeIdCol, {7} AS nodeGroupCol, {1}/1000/60 AS execTimeCol FROM {2} t WHERE {3} = 0  AND {1} <= ? AND {4} = -1 AND NOT EXISTS ({5})",
				randomRangeSQLExpression, getColumnName(TaskModel._TYPECODE, TaskModel.EXECUTIONTIMEMILLIS),
				getTableName(TaskModel._TYPECODE), getColumnName(TaskModel._TYPECODE, TaskModel.FAILED),
				getColumnName(TaskModel._TYPECODE, TaskModel.RUNNINGONCLUSTERNODE), conditionSubQuery, nodeIdExpression,
				nodeGroupExpression);
	}

	private String getTableName(final String code)
	{
		final ComposedTypeModel type = typeService.getComposedTypeForCode(code);
		return type.getTable();
	}

	private String getColumnName(final String code, final String column)
	{
		final ComposedTypeModel type = typeService.getComposedTypeForCode(code);
		return type.getDeclaredattributedescriptors().stream().filter(s -> s.getQualifier().equalsIgnoreCase(column)).findFirst()
		           .orElseThrow(() -> new IllegalArgumentException("type " + code + " is not recognizable")).getDatabaseColumn();
	}

	@Override
	public void afterPropertiesSet()
	{
		this.activateWorkersTimer = metricRegistry.timer(
				RuntimeConfigHolder.metricName("pooling.scheduler.activateWorkers.time"));
		this.lockTimer = metricRegistry.timer(RuntimeConfigHolder.metricName("pooling.scheduler.lock.time"));
		this.schedulerTimer = metricRegistry.timer(RuntimeConfigHolder.metricName("pooling.scheduler.time"));
		this.deactivateWorkersTimer = metricRegistry
				.timer(RuntimeConfigHolder.metricName("pooling.scheduler.deactivateWorkers.time"));
		this.copyTasksTimer = metricRegistry.timer(RuntimeConfigHolder.metricName("pooling.scheduler.copyTasks.time"));
		this.countTasksTimer = metricRegistry.timer(RuntimeConfigHolder.metricName("pooling.scheduler.countTasks.time"));
	}

	@Override
	@Required
	public void setGatewayFactory(final AuxiliaryTablesGatewayFactory gatewayFactory)
	{
		this.gatewayFactory = gatewayFactory;
	}

	@Override
	@Required
	public void setMetricRegistry(final MetricRegistry metricRegistry)
	{
		this.metricRegistry = metricRegistry;
	}

	@Override
	@Required
	public void setTypeService(final TypeService typeService)
	{
		this.typeService = typeService;
	}
}

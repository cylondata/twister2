//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.comms.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.utils.WorkerResourceUtils;

/**
 * This class can be used to build an instance of {@link LogicalPlan}
 * {@link LogicalPlanBuilder} can distribute the tasks in 3 modes.
 *
 * <ol>
 *   <li>Fair distribution : Tasks will be evenly distributed among all workers</li>
 *   <li>Fair distribution on Group : Tasks will be evenly distributed
 *   among a specified set of workers</li>
 *   <li>Custom Distribution : User can provide a custom {@link Distribution} logic to
 *   distribute the tasks among the workers</li>
 * </ol>
 */
public final class LogicalPlanBuilder {

  private static AtomicInteger counter = new AtomicInteger(0);
  /**
   * The sources of the plan
   */
  private Set<Integer> sources = new HashSet<>();
  /**
   * target ids of the plan
   */
  private Set<Integer> targets = new HashSet<>();

  /**
   * Source to worker mapping
   */
  private Map<Integer, Integer> sourceToWorker = new HashMap<>();

  /**
   * Target to worker mapping
   */
  private Map<Integer, Integer> targetToWorker = new HashMap<>();

  /**
   * Weather sources are distributed
   */
  private boolean sourcesDistributed;

  /**
   * Weather targets are distributed, we don't distribute after this
   */
  private boolean targetsDistributed;

  /**
   * Worker environment
   */
  private WorkerEnvironment workerEnvironment;

  /**
   * ALl workers ids
   */
  private Set<Integer> allWorkers;

  private LogicalPlanBuilder(int sourcesCount,
                             int targetsCount,
                             WorkerEnvironment workerEnvironment) {
    for (int i = 0; i < sourcesCount; i++) {
      this.sources.add(counter.getAndIncrement());
    }

    for (int i = 0; i < targetsCount; i++) {
      this.targets.add(counter.getAndIncrement());
    }

    this.workerEnvironment = workerEnvironment;
    this.allWorkers = this.workerEnvironment.getWorkerList().stream()
        .map(JobMasterAPI.WorkerInfo::getWorkerID).collect(Collectors.toSet());
  }

  /**
   * Create the plan according the the configurations
   *
   * @param sources number of sources
   * @param targets number of targets
   * @param workerEnvironment the environment
   * @return a configured <code>LogicalPlanBuilder</code>
   */
  public static LogicalPlanBuilder plan(int sources,
                                        int targets,
                                        WorkerEnvironment workerEnvironment) {
    return new LogicalPlanBuilder(sources, targets, workerEnvironment);
  }

  /**
   * Get the source ids from this logical plan builder
   * @return return the set of sources
   */
  public Set<Integer> getSources() {
    return sources;
  }

  /**
   * Return the targets of this logical plan builder
   * @return the set of targets
   */
  public Set<Integer> getTargets() {
    return targets;
  }

  /**
   * Returns the set of sources scheduled on this worker.
   * This method should be called only after doing the distribution
   */
  public Set<Integer> getSourcesOnThisWorker() {
    return this.getXOnWorker(sourceToWorker, workerEnvironment.getWorkerId());
  }

  /**
   * Returns the set of targets scheduled on this worker.
   * This method should be called only after doing the distribution.
   */
  public Set<Integer> getTargetsOnThisWorker() {
    return this.getXOnWorker(targetToWorker, workerEnvironment.getWorkerId());
  }

  /**
   * Returns the set of sources scheduled on specified worker.
   * This method should be called only after doing the distribution.
   */
  public Set<Integer> getSourcesOnWorker(int workerId) {
    return this.getXOnWorker(sourceToWorker, workerId);
  }

  /**
   * Returns the set of targets scheduled on specified worker.
   * This method should be called only after doing the distribution.
   */
  public Set<Integer> getTargetsOnWorker(int workerId) {
    return this.getXOnWorker(targetToWorker, workerId);
  }

  private Set<Integer> getXOnWorker(Map<Integer, Integer> xToWorker, int worker) {
    if (!this.sourcesDistributed || !this.targetsDistributed) {
      throw new RuntimeException("Attempt to read plan before doing the distribution");
    }
    Set<Integer> xOnWorker = new HashSet<>();
    xToWorker.forEach((x, workerId) -> {
      if (workerId == worker) {
        xOnWorker.add(x);
      }
    });
    return xOnWorker;
  }

  /**
   * Builds the {@link LogicalPlan}
   */
  public LogicalPlan build() {
    if (!sourcesDistributed) {
      this.withFairSourceDistribution();
    }

    if (!targetsDistributed) {
      this.withFairTargetDistribution();
    }

    Map<Integer, Set<Integer>> workerToTasks = this.allWorkers.stream()
        .collect(Collectors.toMap(workerId -> workerId, workerId -> new HashSet<>()));

    sourceToWorker.forEach((source, worker) -> {
      workerToTasks.get(worker).add(source);
    });
    targetToWorker.forEach((target, worker) -> {
      workerToTasks.get(worker).add(target);
    });

    Map<Integer, Set<Integer>> groupsToWorkers = new HashMap<>();
    Map<String, Set<Integer>> nodeToTasks = new HashMap<>();

    Map<String, List<JobMasterAPI.WorkerInfo>> containersPerNode =
        WorkerResourceUtils.getWorkersPerNode(workerEnvironment.getWorkerList());

    int i = 0;
    for (Map.Entry<String, List<JobMasterAPI.WorkerInfo>> entry : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (JobMasterAPI.WorkerInfo workerInfo : entry.getValue()) {
        executorsOfGroup.add(workerInfo.getWorkerID());
        Set<Integer> tasksInNode = nodeToTasks.computeIfAbsent(
            workerInfo.getNodeInfo().getNodeIP(),
            k -> new HashSet<>());
        tasksInNode.addAll(workerToTasks.get(workerInfo.getWorkerID()));
      }
      groupsToWorkers.put(i, executorsOfGroup);
      i++;
    }

    return new LogicalPlan(
        workerToTasks, groupsToWorkers,
        nodeToTasks, workerEnvironment.getWorkerId()
    );
  }

  /**
   * Create plan with custom distribution
   * @param distribution the distribution
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withCustomSourceDistribution(Distribution distribution) {
    this.withCustomXDistribution(distribution, sources, sourceToWorker);
    this.sourcesDistributed = true;
    return this;
  }

  /**
   * Create plan with custom distribution
   * @param distribution the distribution
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withCustomTargetDistribution(Distribution distribution) {
    this.withCustomXDistribution(distribution, targets, targetToWorker);
    this.targetsDistributed = true;
    return this;
  }

  /**
   * This method will fairly distribute all sources and targets among the specified set of workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairDistribution(Set<Integer> groupOfWorkers) {
    this.withFairTargetDistribution(groupOfWorkers);
    this.withFairSourceDistribution(groupOfWorkers);
    return this;
  }

  /**
   * This method will fairly distribute all sources and targets among the available workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairDistribution() {
    this.withFairTargetDistribution();
    this.withFairSourceDistribution();
    return this;
  }

  /**
   * This method will distribute sources fairly among the available set of workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairSourceDistribution() {
    this.withFairSourceDistribution(allWorkers);
    return this;
  }

  /**
   * This method will distribute targets fairly among the available set of workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairTargetDistribution() {
    this.withFairTargetDistribution(allWorkers);
    return this;
  }

  /**
   * This method will distribute targets fairly among the specified group of workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairSourceDistribution(Set<Integer> groupOfWorkers) {
    this.withFairXDistribution(sources, sourceToWorker, groupOfWorkers);
    this.sourcesDistributed = true;
    return this;
  }

  /**
   * This method will distribute sources fairly among the specified group of workers
   * @return <code>LogicalPlanBuilder</code>
   */
  public LogicalPlanBuilder withFairTargetDistribution(Set<Integer> groupOfWorkers) {
    this.withFairXDistribution(targets, targetToWorker, groupOfWorkers);
    this.targetsDistributed = true;
    return this;
  }

  private void validateWorkerGroup(Set<Integer> groupOfWorkers) {
    if (!this.allWorkers.containsAll(groupOfWorkers)) {
      throw new RuntimeException("Group of workers specified contains invalid worker ids.");
    }
  }

  private LogicalPlanBuilder withFairXDistribution(Set<Integer> allOfKind,
                                                   Map<Integer, Integer> xToWorkerMap,
                                                   Set<Integer> workers) {
    this.validateWorkerGroup(workers);

    int excess = allOfKind.size() % workers.size() > 0 ? 1 : 0;
    final int tasksPerWorker = (allOfKind.size() / workers.size()) + excess;

    final Queue<Integer> tasksQueue = new LinkedList<>(allOfKind);

    workers.forEach(workerId -> {
      for (int i = 0; i < tasksPerWorker; i++) {
        if (!tasksQueue.isEmpty()) {
          xToWorkerMap.put(tasksQueue.poll(), workerId);
        }
      }
    });
    return this;
  }

  private LogicalPlanBuilder withCustomXDistribution(Distribution distribution,
                                                     Set<Integer> allOfKind,
                                                     Map<Integer, Integer> xToWorkerMap) {
    allOfKind.forEach(source -> {
      int worker = distribution.stickTaskTo(source, allWorkers, allOfKind);
      xToWorkerMap.put(source, worker);
    });
    return this;
  }

  public interface Distribution {
    /**
     * This method should return the workerId to schedule the task
     *
     * @param task task to be scheduled
     * @param workers set of all available workers as a reference
     * @param allTasksOfKind all other tasks of type "task"
     * @return worker id to schedule the task
     */
    int stickTaskTo(int task, Set<Integer> workers, Set<Integer> allTasksOfKind);
  }
}

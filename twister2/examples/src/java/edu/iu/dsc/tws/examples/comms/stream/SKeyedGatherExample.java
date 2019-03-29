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
package edu.iu.dsc.tws.examples.comms.stream;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.comms.api.stream.SKeyedGather;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

/**
 * todo not applicable for streaming, without window
 */
public class SKeyedGatherExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(SKeyedGatherExample.class.getName());

  private SKeyedGather keyedGather;

  private boolean gatherDone;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Set<Integer> targets = new HashSet<>();
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    keyedGather = new SKeyedGather(communicator, taskPlan, sources, targets,
        MessageType.OBJECT, MessageType.OBJECT,
        new GatherBulkReceiver(), new LoadBalanceSelector());

    Set<Integer> sourceTasks = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : sourceTasks) {
      finishedSources.put(t, false);
    }
    if (sourceTasks.size() == 0) {
      sourcesDone = true;
    }

    Set<Integer> sinkTasks = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 1);

    LOG.log(Level.INFO, String.format("Worker[%d], Source Tasks %s , Sink Tasks %s",
        workerId, sourceTasks, sinkTasks));
    // now initialize the workers
    for (int t : sourceTasks) {
      // the map thread where data is produced
      MapWorker mapWorker = new MapWorker(t);
      mapWorker.setTimingForLowestTargetOnly(true);
      Thread mapThread = new Thread(mapWorker);
      mapThread.start();
    }

  }

  @Override
  protected void progressCommunication() {
    keyedGather.progress();
  }

  @Override
  protected boolean isDone() {
    return gatherDone && sourcesDone && !keyedGather.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!keyedGather.reduce(task, key, data, flag)) {
      // lets wait a litte and try again
      keyedGather.progress();
    }
    return true;
  }

  public class GatherBulkReceiver implements BulkReceiver {
    private int count = 0;
    private int countToLowest = 0;

    //expected for timing target
    private int expectedIterations;
    private int warmupIterations;

    //for all targets
    private int expectedTotalIterations;

    private int lowestTarget = -1;

    private int getExpectedForId(int iterations, int id, int lowestId, int totalIds,
                                 int totalSource) {
      int adjustedId = id - lowestId;
      int total = iterations / totalIds;
      if (iterations % totalIds > 0 && iterations % totalIds > adjustedId) {
        total++;
      }
      return total * totalSource;
    }

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      if (expectedIds.isEmpty()) {
        gatherDone = true;
        return;
      }
      this.lowestTarget = expectedIds.stream().min(
          Comparator.comparingInt(o -> (Integer) o)
      ).get();
      int totalSources = jobParameters.getTaskStages().get(0);
      expectedTotalIterations = expectedIds.stream().map(id -> getExpectedForId(
          jobParameters.getTotalIterations(),
          id,
          lowestTarget,
          jobParameters.getTaskStages().get(1),
          totalSources
      )).reduce(0, (integer, integer2) -> integer + integer2);
      expectedIterations = getExpectedForId(
          jobParameters.getTotalIterations(),
          lowestTarget,
          lowestTarget,
          jobParameters.getTaskStages().get(1),
          totalSources
      );
      warmupIterations = getExpectedForId(
          jobParameters.getTotalIterations(),
          lowestTarget,
          lowestTarget,
          jobParameters.getTaskStages().get(1),
          totalSources
      );
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      count++;
      //do timing and benchmark only on lowest target
      if (target == lowestTarget && workerId == 0) {
        System.out.println(count + "," + countToLowest);
        countToLowest++;
        if (countToLowest > warmupIterations) {
          Timing.mark(TIMING_MESSAGE_RECV, workerId == 0);
        }

        if (countToLowest == expectedIterations + warmupIterations) {
          Timing.mark(TIMING_ALL_RECV, workerId == 0);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, workerId == 0);
          resultsRecorder.writeToCSV();
          LOG.info(() -> String.format("Target %d received count %d", target, count));
        }
      }
      if (expectedTotalIterations == count) {
        gatherDone = true;
      }
      System.out.println("Total count : " + count + " , WorkerID : " + workerId);
      return true;
    }
  }
}

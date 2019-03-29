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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.stream.SDirect;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;

public class SDirectExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SDirectExample.class.getName());

  private SDirect direct;

  private boolean directDone = true;

  private int receiverInWorker0;

  private ResultsVerifier<int[], int[]> resultsVerifier;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    List<Integer> sources = new ArrayList<>();
    List<Integer> targets = new ArrayList<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    // create the communication
    direct = new SDirect(communicator, taskPlan, sources, targets,
        MessageType.INTEGER, new PartitionReceiver());

    Set<Integer> targetTasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 1);
    for (int taskId : targetTasksOfExecutor) {
      if (targets.contains(taskId)) {
        directDone = false;

        if (workerId == 0) {
          receiverInWorker0 = taskId;
        }
      }
    }

    Set<Integer> sourceTasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> ints, IntArrayComparator.getInstance());

    // now initialize the workers
    for (int t : sourceTasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    direct.progress();
  }

  @Override
  protected boolean isDone() {
    return directDone && sourcesDone && !direct.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!direct.partition(task, data, flag)) {
      // lets wait a litte and try again
      direct.progress();
    }
    return true;
  }

  public class PartitionReceiver implements SingularReceiver {
    private int count = 0;
    private int countToLowest = 0;

    private int totalExpectedCount = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      this.totalExpectedCount = targets.size() * jobParameters.getTotalIterations();
    }

    @Override
    public boolean receive(int target, Object object) {
      count++;

      if (receiverInWorker0 == target) {
        this.countToLowest++;
        if (countToLowest > jobParameters.getWarmupIterations()) {
          Timing.mark(BenchmarkConstants.TIMING_MESSAGE_RECV, workerId == 0);
        }
        if (countToLowest == jobParameters.getTotalIterations()) {
          Timing.mark(TIMING_ALL_RECV, workerId == 0);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, workerId == 0);
          resultsRecorder.writeToCSV();
          LOG.info(() -> String.format("Target %d received ALL %d", target, countToLowest));
        }
      }

      if (count == this.totalExpectedCount) {
        directDone = true;
      }

      verifyResults(resultsVerifier, object, null);

      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
  }
}

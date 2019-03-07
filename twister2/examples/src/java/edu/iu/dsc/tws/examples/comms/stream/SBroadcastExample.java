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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.stream.SBroadCast;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

public class SBroadcastExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SBroadcastExample.class.getName());

  private SBroadCast bcast;

  private boolean bCastDone;

  private int receiverInWorker0 = -1;
  private ResultsVerifier<int[], int[]> resultsVerifier;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    if (jobParameters.getTaskStages().get(0) != 1) {
      LOG.warning("Setting task stages to 1");
      jobParameters.getTaskStages().set(0, 1);
    }

    Set<Integer> targets = new HashSet<>();
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 1; i < noOfTargetTasks + 1; i++) {
      targets.add(i);
    }
    int source = 0;

    // create the communication
    bcast = new SBroadCast(communicator, taskPlan, source, targets,
        MessageType.INTEGER, new BCastReceiver());

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    Set<Integer> targetTasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 1);
    for (int taskId : targetTasksOfExecutor) {
      if (targets.contains(taskId)) {
        bCastDone = false;

        if (workerId == 0) {
          receiverInWorker0 = taskId;
        }
      }
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (array, args) -> array, IntArrayComparator.getInstance());

    // the map thread where data is produced
    if (workerId == 0) {
      Thread mapThread = new Thread(new MapWorker(source));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    bcast.progress();
  }

  @Override
  protected boolean isDone() {
    return bCastDone && sourcesDone && !bcast.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!bcast.bcast(task, data, flag)) {
      // lets wait a litte and try again
      bcast.progress();
    }
    return true;
  }

  public class BCastReceiver implements SingularReceiver {

    private int count = 0;
    private int expected = 0;
    private int warmUp = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      expected = targets.size() * jobParameters.getIterations();
      warmUp = targets.size() * jobParameters.getWarmupIterations();
    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count > this.warmUp) {
        Timing.mark(TIMING_MESSAGE_RECV, workerId == 0
            && target == receiverInWorker0);
      }

      verifyResults(resultsVerifier, object, null);

      if (count == expected + warmUp) {
        Timing.mark(TIMING_ALL_RECV, workerId == 0 && target == receiverInWorker0);
        BenchmarkUtils.markTotalAndAverageTime(resultsRecorder,
            workerId == 0 && target == receiverInWorker0);
        resultsRecorder.writeToCSV();
        bCastDone = true;
      }
      return true;
    }
  }
}

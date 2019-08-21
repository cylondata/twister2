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

import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.stream.SBroadCast;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
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

  private boolean bCastDone = true;

  private int receiverInWorker0 = -1;
  private ResultsVerifier<int[], int[]> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    if (jobParameters.getTaskStages().get(0) != 1) {
      LOG.warning("Setting task stages to 1");
      jobParameters.getTaskStages().set(0, 1);
    }

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    bcast = new SBroadCast(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new BCastReceiver());

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    Set<Integer> targetTasksOfExecutor = logicalPlanBuilder.getTargetsOnThisWorker();
    for (int taskId : targetTasksOfExecutor) {
      if (logicalPlanBuilder.getTargets().contains(taskId)) {
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
      Thread mapThread = new Thread(new MapWorker(
          logicalPlanBuilder.getSources().iterator().next()));
      mapThread.start();
    }
  }

  @Override
  protected boolean progressCommunication() {
    return bcast.progress();
  }

  @Override
  protected boolean isDone() {
    return bCastDone && sourcesDone && bcast.isComplete();
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
    private int countToLowest = 0;

    private int totalExpectedCount = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      this.totalExpectedCount = targets.size() * jobParameters.getTotalIterations();
    }


    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (target == receiverInWorker0) {
        this.countToLowest++;
        if (this.countToLowest > jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_MESSAGE_RECV, workerId == 0
              && target == receiverInWorker0);
        }

        verifyResults(resultsVerifier, object, null);

        if (countToLowest == jobParameters.getTotalIterations()) {
          Timing.mark(TIMING_ALL_RECV, workerId == 0 && target == receiverInWorker0);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder,
              workerId == 0 && target == receiverInWorker0);
          resultsRecorder.writeToCSV();
        }
      }

      if (count == this.totalExpectedCount) {
        bCastDone = true;
      }
      return true;
    }
  }
}

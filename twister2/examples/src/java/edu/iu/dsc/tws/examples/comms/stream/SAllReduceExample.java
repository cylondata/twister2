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

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.comms.stream.SAllReduce;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.GeneratorUtils;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

public class SAllReduceExample extends BenchWorker {

  private static final Logger LOG = Logger.getLogger(SAllReduceExample.class.getName());

  private SAllReduce reduce;

  private boolean reduceDone = true;

  private ResultsVerifier<int[], int[]> resultsVerifier;

  private int receiverInWorker0 = -1; //any recv scheduled in worker 0

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    reduce = new SAllReduce(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new ReduceOperationFunction(Op.SUM, MessageTypes.INTEGER_ARRAY),
        new FinalSingularReceiver());

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }

    sourcesDone = tasksOfExecutor.size() == 0;

    Set<Integer> targetTasksOfExecutor = logicalPlanBuilder.getTargetsOnThisWorker();
    for (int taskId : targetTasksOfExecutor) {
      if (logicalPlanBuilder.getTargets().contains(taskId)) {
        reduceDone = false;
      }

      if (workerId == 0) {
        receiverInWorker0 = taskId;
      }
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (array, args) -> {
      int sourcesCount = jobParameters.getTaskStages().get(0);
      return GeneratorUtils.multiplyIntArray(array, sourcesCount);
    }, IntArrayComparator.getInstance());

    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected boolean progressCommunication() {
    reduce.progress();
    return !reduce.isComplete();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!reduce.reduce(task, data, flag)) {
      // lets wait a litte and try again
      reduce.progress();
    }
    return true;
  }

  @Override
  protected boolean isDone() {
    return reduceDone && sourcesDone && reduce.isComplete();
  }

  public class FinalSingularReceiver implements SingularReceiver {

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
          Timing.mark(TIMING_MESSAGE_RECV, workerId == 0 && target == receiverInWorker0);
        }

        if (countToLowest == jobParameters.getTotalIterations()) {
          Timing.mark(TIMING_ALL_RECV, workerId == 0 && target == receiverInWorker0);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder,
              workerId == 0 && target == receiverInWorker0);
          resultsRecorder.writeToCSV();
        }
      }

      LOG.info(() -> String.format("Target %d received count %d", target, count));

      verifyResults(resultsVerifier, object, null);

      if (count == this.totalExpectedCount) {
        reduceDone = true;
      }

      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    reduce.finish(src);
  }

  @Override
  public void close() {
    reduce.close();
  }
}

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
import edu.iu.dsc.tws.comms.stream.SReduce;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

public class SReduceExample extends BenchWorker {

  private static final Logger LOG = Logger.getLogger(SReduceExample.class.getName());

  private SReduce reduce;

  private boolean reduceDone = false;

  private ResultsVerifier<int[], int[]> resultsVerifier;

  @Override
  protected void compute(WorkerEnvironment workerEnv) {
    if (jobParameters.getTargets() != 1) {
      LOG.warning("Setting targets to 1. Found, " + jobParameters.getTargets());
      jobParameters.getTaskStages().set(1, 1);
    }

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    reduce = new SReduce(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new ReduceOperationFunction(Op.SUM, MessageTypes.INTEGER_ARRAY),
        new FinalSingularReceiver()
    );

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }

    sourcesDone = tasksOfExecutor.size() == 0;

    reduceDone = !logicalPlan.getLogicalIdsOfWorker(workerId).contains(
        logicalPlanBuilder.getTargets().iterator().next()
    );

    //generating the expectedIterations results at the end

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray, (array, args) -> {
      int sourcesCount = jobParameters.getTaskStages().get(0);
      int[] outArray = new int[array.length];
      for (int i = 0; i < array.length; i++) {
        outArray[i] = array[i] * sourcesCount;
      }
      return outArray;
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
      // lets wait a little and try again
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

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      Timing.defineFlag(TIMING_MESSAGE_RECV, jobParameters.getIterations(), workerId == 0);
    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count > jobParameters.getWarmupIterations()) {
        Timing.mark(TIMING_MESSAGE_RECV, workerId == 0);
      }

      LOG.info(() -> String.format("Target %d received count %d", target, count));

      verifyResults(resultsVerifier, object, null);

      if (count == jobParameters.getTotalIterations()) {
        Timing.mark(TIMING_ALL_RECV, workerId == 0);
        BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, workerId == 0);
        resultsRecorder.writeToCSV();
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

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
package edu.iu.dsc.tws.examples.comms.batch;

import java.util.Comparator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BAllReduce;
import edu.iu.dsc.tws.comms.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.GeneratorUtils;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;

public class BAllReduceExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(BAllReduceExample.class.getName());

  private BAllReduce reduce;

  private ResultsVerifier<int[], int[]> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    Set<Integer> sources = IntStream.range(0, noOfSourceTasks).boxed().collect(Collectors.toSet());

    int noOfTargetTasks = jobParameters.getTaskStages().get(1);
    Set<Integer> targets = IntStream.range(noOfSourceTasks, noOfTargetTasks + noOfSourceTasks)
        .boxed().collect(Collectors.toSet());

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    reduce = new BAllReduce(workerEnv.getCommunicator(), logicalPlanBuilder,
        new ReduceOperationFunction(Op.SUM, MessageTypes.INTEGER_ARRAY),
        new FinalSingularReceiver(),
        MessageTypes.INTEGER_ARRAY);

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();

    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> GeneratorUtils.multiplyIntArray(ints,
            jobParameters.getIterations() * sources.size()),
        IntArrayComparator.getInstance());

    LOG.log(Level.INFO, String.format("%d Sources %s target %s this %s",
        workerId, sources, targets, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    reduce.close();
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
    return sourcesDone && reduce.isComplete();
  }

  public class FinalSingularReceiver implements SingularReceiver {
    private int lowestTarget = 0;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      if (targets.isEmpty()) {
        return;
      }
      this.lowestTarget = targets.stream().min(Comparator.comparingInt(o -> (Integer) o)).get();
    }

    @Override
    public boolean receive(int target, Object object) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV,
          workerId == 0 && target == lowestTarget);
      BenchmarkUtils.markTotalTime(resultsRecorder, workerId == 0
          && target == lowestTarget);
      resultsRecorder.writeToCSV();
      verifyResults(resultsVerifier, object, null);
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    reduce.finish(src);
  }
}

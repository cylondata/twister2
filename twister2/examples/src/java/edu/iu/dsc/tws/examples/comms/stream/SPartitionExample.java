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
import edu.iu.dsc.tws.comms.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.comms.stream.SPartition;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;

/**
 * todo add timing, and terminating conditions
 */
public class SPartitionExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SPartitionExample.class.getName());

  private SPartition partition;

  private ResultsVerifier<int[], int[]> resultsVerifier;

  private int recvrInWorker0 = -1;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    partition = new SPartition(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER_ARRAY, new PartitionReceiver(), new LoadBalanceSelector());

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> ints, IntArrayComparator.getInstance());

    logicalPlanBuilder.getTargetsOnThisWorker().forEach(t -> {
      if (workerId == 0) {
        recvrInWorker0 = t;
      }
    });

    Set<Integer> tasksOfExecutor = logicalPlanBuilder.getSourcesOnThisWorker();
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected boolean progressCommunication() {
    partition.progress();
    return !partition.isComplete();
  }

  @Override
  protected boolean isDone() {
    return sourcesDone && partition.isComplete();
  }

  @Override
  public void close() {
    partition.close();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!partition.partition(task, data, flag)) {
      // lets wait a litte and try again
      partition.progress();
    }
    return true;
  }

  public class PartitionReceiver implements SingularReceiver {

    private int count = 0;
    private int expected;
    private int expectedWarmups;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      expected = (jobParameters.getTotalIterations() * jobParameters.getTaskStages().get(0)
          / jobParameters.getTaskStages().get(1)) * targets.size();
      //roughly, could be more than this

      expectedWarmups = (jobParameters.getWarmupIterations()
          * jobParameters.getTaskStages().get(0)
          / jobParameters.getTaskStages().get(1)) * targets.size();
    }

    @Override
    public boolean receive(int target, Object it) {
      count += 1;
//      LOG.log(Level.INFO, String.format("%d Received message %d count %d expected %d",
//          workerId, target, count, expected));
      //Since this is a streaming example we will simply stop after a number of messages are
      // received
      if (count >= expectedWarmups) {
        Timing.mark(TIMING_MESSAGE_RECV, workerId == 0 && target == recvrInWorker0);
      }

      if (count >= expected) {
        Timing.mark(TIMING_ALL_RECV, workerId == 0
            && target == recvrInWorker0);
        BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, workerId == 0
            && target == recvrInWorker0);
        resultsRecorder.writeToCSV();
      }

      verifyResults(resultsVerifier, it, null);

      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    partition.finish(src);
  }
}

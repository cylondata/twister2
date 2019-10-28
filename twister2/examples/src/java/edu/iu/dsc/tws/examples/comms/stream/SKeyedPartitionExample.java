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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.stream.SKeyedPartition;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;

/**
 * Streaming keyed partition example
 * todo add timing and terminating condition
 */
public class SKeyedPartitionExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(SPartitionExample.class.getName());

  private SKeyedPartition partition;

  private ResultsVerifier<int[], Tuple<Integer, int[]>> resultsVerifier;

  @Override
  protected void execute(WorkerEnvironment workerEnv) {
    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        workerEnv
    ).withFairDistribution();

    // create the communication
    partition = new SKeyedPartition(workerEnv.getCommunicator(), logicalPlanBuilder,
        MessageTypes.INTEGER, MessageTypes.INTEGER_ARRAY,
        new PartitionReceiver(), new HashingSelector());

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> new Tuple<>(-1, ints),
        new TupleComparator<>(
            (d1, d2) -> true, //any int
            IntArrayComparator.getInstance()
        ));

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
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!partition.partition(task, key, data, flag)) {
      // lets wait a litte and try again
      partition.progress();
    }
    return true;
  }

  public class PartitionReceiver implements SingularReceiver {
    private int count = 0;
    private int expected;

    @Override
    public void init(Config cfg, Set<Integer> targets) {
      expected = targets.size() * jobParameters.getIterations();
    }

    @Override
    public boolean receive(int target, Object data) {
      count++;
      LOG.log(Level.INFO, String.format("%d Received message %d count %d expected %d",
          workerId, target, count, expected));
      // verifyResults(resultsVerifier, data, null);
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    partition.finish(src);
  }
}

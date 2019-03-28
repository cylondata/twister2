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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.api.stream.SKeyedPartition;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.Utils;
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

  private boolean partitionDone = false;
  private ResultsVerifier<int[], Tuple<Integer, int[]>> resultsVerifier;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Set<Integer> targets = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    // create the communication
    partition = new SKeyedPartition(communicator, taskPlan, sources, targets,
        MessageType.INTEGER, MessageType.INTEGER,
        new PartitionReceiver(), new HashingSelector());

    this.resultsVerifier = new ResultsVerifier<>(inputDataArray,
        (ints, args) -> new Tuple<>(-1, ints),
        new TupleComparator<>(
            (d1, d2) -> true, //any int
            IntArrayComparator.getInstance()
        ));

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    partition.progress();
  }

  @Override
  protected boolean isDone() {
    return partitionDone && sourcesDone && !partition.hasPending();
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
      if (count >= expected) {
        partitionDone = true;
      }
      // verifyResults(resultsVerifier, data, null);
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    partition.finish(src);
  }
}

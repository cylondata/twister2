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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.batch.BPartitionBasedKeyedReduce;
import edu.iu.dsc.tws.comms.op.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.comms.op.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;

/**
 * This class is a example use of the partition based keyed reduce function
 */
public class BKeyedPartitionBasedReduceExample extends KeyedBenchWorker {

  private static final Logger LOG = Logger.getLogger(
      BKeyedPartitionBasedReduceExample.class.getName());

  private BPartitionBasedKeyedReduce partitionBasedKeyedReduce;

  private boolean partitionDone = false;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
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
    partitionBasedKeyedReduce = new BPartitionBasedKeyedReduce(communicator, taskPlan, sources,
        targets, MessageType.INTEGER, MessageType.INTEGER, new PartitionReceiver(),
        new SimpleKeyBasedSelector(), new ReduceOperationFunction(Op.SUM, MessageType.INTEGER));

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    // now initialize the workers

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    partitionBasedKeyedReduce.progress();
  }

  @Override
  protected boolean isDone() {
    return partitionDone && sourcesDone && !partitionBasedKeyedReduce.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!partitionBasedKeyedReduce.partition(task, key, data, flag)) {
      // lets wait a litte and try again
      partitionBasedKeyedReduce.progress();
    }
    return true;
  }

  public class PartitionReceiver implements BulkReceiver {
    private int count = 0;
    private int expected;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
      expected = expectedIds.size() * jobParameters.getIterations();
    }

    @Override
    @SuppressWarnings("unchecked")

    public boolean receive(int target, Iterator<Object> it) {
      if (it == null) {
        return true;
      }
      while (it.hasNext()) {
        ImmutablePair<Object, Object> currentPair = (ImmutablePair) it.next();
        Object key = currentPair.getKey();
        int[] data = (int[]) currentPair.getValue();
        LOG.log(Level.INFO, String.format("%d Results : key: %s value: %s", workerId, key,
            Arrays.toString(Arrays.copyOfRange(data, 0, Math.min(data.length, 10)))));
      }
      partitionDone = true;

      //TODO: need to update the verification code
//      experimentData.setOutput(object);
//      try {
//        verify();
//      } catch (VerificationException e) {
//        LOG.info("Message : " + e.getMessage());
//      }
      return true;
    }
  }

  @Override
  protected void finishCommunication(int src) {
    partitionBasedKeyedReduce.finish(src);
  }
}

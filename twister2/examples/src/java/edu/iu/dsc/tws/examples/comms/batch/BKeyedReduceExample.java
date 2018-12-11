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
import edu.iu.dsc.tws.comms.op.batch.BKeyedReduce;
import edu.iu.dsc.tws.comms.op.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.comms.op.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;
import edu.iu.dsc.tws.examples.verification.ExperimentVerification;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;

/**
 * Created by pulasthi on 8/24/18.
 */
public class BKeyedReduceExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(BKeyedReduceExample.class.getName());

  private BKeyedReduce keyedReduce;

  private boolean reduceDone;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, workerId,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    Set<Integer> targets = new HashSet<>();
    Integer noOfTargetTasks = jobParameters.getTaskStages().get(1);
    for (int i = 0; i < noOfTargetTasks; i++) {
      targets.add(noOfSourceTasks + i);
    }

    keyedReduce = new BKeyedReduce(communicator, taskPlan, sources, targets,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER),
        new FinalBulkReceiver(), MessageType.INTEGER, MessageType.INTEGER,
        new SimpleKeyBasedSelector());

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }
    reduceDone = true;
    for (int target : targets) {
      if (taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
        reduceDone = false;
      }
    }


    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new KeyedBenchWorker.MapWorker(t));
      mapThread.start();
    }

  }

  @Override
  public void close() {
    keyedReduce.close();
  }

  @Override
  protected void progressCommunication() {
    keyedReduce.progress();
  }

  @Override
  protected boolean isDone() {
    return reduceDone && sourcesDone && !keyedReduce.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!keyedReduce.reduce(task, key, data, flag)) {
      // lets wait a litte and try again
      keyedReduce.progress();
    }
    return true;
  }

  public class FinalBulkReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
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
      reduceDone = true;

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

  public void verify() throws VerificationException {
    boolean doVerify = jobParameters.isDoVerify();
    boolean isVerified = false;
    if (doVerify) {
      LOG.info("Verifying results ...");
      ExperimentVerification experimentVerification
          = new ExperimentVerification(experimentData, OperationNames.KEYED_REDUCE);
      isVerified = experimentVerification.isVerified();
      if (isVerified) {
        LOG.info("Results generated from the experiment are verified.");
      } else {
        throw new VerificationException("Results do not match");
      }
    }
  }

  @Override
  protected void finishCommunication(int src) {
    keyedReduce.finish(src);
  }
}

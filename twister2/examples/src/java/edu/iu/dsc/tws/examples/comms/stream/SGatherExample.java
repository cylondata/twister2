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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.stream.SGather;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.BenchWorker;
import edu.iu.dsc.tws.examples.verification.ExperimentVerification;
import edu.iu.dsc.tws.examples.verification.VerificationException;
import edu.iu.dsc.tws.executor.core.OperationNames;

public class SGatherExample extends BenchWorker {
  private static final Logger LOG = Logger.getLogger(SGatherExample.class.getName());

  private SGather reduce;

  private boolean reduceDone = false;

  @Override
  protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;

    // create the communication
    reduce = new SGather(communicator, taskPlan, sources, target, MessageType.INTEGER,
        new FinalReduceReceiver(jobParameters.getIterations()));


    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
      reduceDone = true;
    }

    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new BenchWorker.MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  protected void progressCommunication() {
    reduce.progress();
  }

  @Override
  protected boolean sendMessages(int task, Object data, int flag) {
    while (!reduce.gather(task, data, flag)) {
      // lets wait a litte and try again
      reduce.progress();
    }
    return true;
  }

  @Override
  protected boolean isDone() {
//    LOG.log(Level.INFO, String.format("%d Reduce %b sources %b pending %b",
//        workerId, reduceDone, sourcesDone, reduce.hasPending()));
    return reduceDone && sourcesDone && !reduce.hasPending();
  }

  public class FinalReduceReceiver implements MessageReceiver {
    private int count = 0;
    private int expected;

    public FinalReduceReceiver(int expected) {
      this.expected = expected;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      if (object instanceof List) {
        count += ((List) object).size();
        ArrayList<?> a = (ArrayList<?>) object;
        experimentData.setOutput(a.get(0));
      }

      if (count == expected) {
        LOG.log(Level.INFO, String.format("Target %d received count %d", target, count));
        reduceDone = true;
      }

      try {
        verify();
      } catch (VerificationException e) {
        LOG.info("Exception Message : " + e.getMessage());
      }

      return true;
    }

    @Override
    public boolean progress() {
      return true;
    }
  }

  public void verify() throws VerificationException {
    boolean doVerify = jobParameters.isDoVerify();
    boolean isVerified = false;
    if (doVerify) {
      LOG.info("Verifying results ...");
      ExperimentVerification experimentVerification
          = new ExperimentVerification(experimentData, OperationNames.GATHER);
      isVerified = experimentVerification.isVerified();
      if (isVerified) {
        LOG.info("Results generated from the experiment are verified.");
      } else {
        throw new VerificationException("Results do not match");
      }
    }
  }
}

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BKeyedGather;
import edu.iu.dsc.tws.comms.api.selectors.SimpleKeyBasedSelector;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.KeyedBenchWorker;

public class BDKeyedGatherExample extends KeyedBenchWorker {
  private static final Logger LOG = Logger.getLogger(BDKeyedGatherExample.class.getName());

  private BKeyedGather keyedGather;

  private boolean gatherDone;

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
    // create the communication
    keyedGather = new BKeyedGather(communicator, taskPlan, sources, targets,
        MessageType.INTEGER, MessageType.INTEGER, new FinalReduceReceiver(),
        Comparator.comparing(o -> (Integer) o),
        new SimpleKeyBasedSelector(), true);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);

    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    gatherDone = true;
    for (int target : targets) {
      if (taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
        gatherDone = false;
      }
    }

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, 1, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

  @Override
  public void close() {
    keyedGather.close();
  }

  @Override
  protected void progressCommunication() {
    keyedGather.progress();
  }

  @Override
  protected boolean isDone() {
    return gatherDone && sourcesDone && !keyedGather.hasPending();
  }

  @Override
  protected boolean sendMessages(int task, Object key, Object data, int flag) {
    while (!keyedGather.gather(task, key, data, flag)) {
      // lets wait a litte and try again
      keyedGather.progress();
    }
    return true;
  }

  @Override
  protected void finishCommunication(int src) {
    keyedGather.finish(src);
  }

  public class FinalReduceReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean receive(int target, Iterator<Object> it) {
      LOG.log(Level.INFO, String.format("%d Received final input", workerId));

      if (it == null) {
        return true;
      }

      while (it.hasNext()) {
        Tuple<Object, Object> currentPair = (Tuple) it.next();
        Object key = currentPair.getKey();
        Iterator dataIterator = (Iterator) currentPair.getValue();
        int[] data = new int[0];
        if (dataIterator.hasNext()) {
          data = (int[]) dataIterator.next();
        }
        LOG.log(Level.INFO, String.format("%d Results : key: %s value sample: %s num vals : %s",
            workerId, key, Arrays.toString(Arrays.copyOfRange(data,
                0, Math.min(data.length, 10))), 1));
      }
      gatherDone = true;
      return true;
    }
  }
}

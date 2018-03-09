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
package edu.iu.dsc.tws.examples.basic.comms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseBroadcastCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseBroadcastCommunication.class.getName());

  private DataFlowOperation broadcast;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 32;

  private int noOfTasksPerExecutor = 2;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());
    try {
      this.config = cfg;
      this.resourcePlan = plan;
      this.id = containerId;
      this.status = Status.INIT;
      this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();

      // lets create the task plan
      TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
      LOG.log(Level.INFO, "Task plan: " + taskPlan);
      //first get the communication config file
      TWSNetwork network = new TWSNetwork(cfg, taskPlan);

      TWSCommunication channel = network.getDataFlowTWSCommunication();

      Set<Integer> sources = new HashSet<>();
      for (int i = 0; i < NO_OF_TASKS; i++) {
        sources.add(i);
      }
      int dest = NO_OF_TASKS;

      Map<String, Object> newCfg = new HashMap<>();

      LOG.info(String.format("Setting up reduce dataflow operation %d %s", dest, sources));
      // this method calls the init method
      // I think this is wrong
      broadcast = channel.broadCast(newCfg, MessageType.OBJECT, 0, dest,
          sources, new BCastReceive());

      // the map thread where data is produced
      if (id == 0) {
        Thread mapThread = new Thread(new MapWorker());
        mapThread.start();
      }

      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          broadcast.progress();
          Thread.yield();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int sendCount = 0;
    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker");
      for (int i = 0; i < 1000; i++) {
        IntData data = generateData();
        data.setId(i);
        // lets generate a message
//        LOG.info("Sending message from task:" + NO_OF_TASKS);
        while (!broadcast.send(NO_OF_TASKS, data, 0)) {
          // lets wait a litte and try again
          broadcast.progress();
//          try {
//            Thread.sleep(1);
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
        }
//        LOG.info(String.format("%d sending from %d", id, NO_OF_TASKS)
//            + " count: " + sendCount++);
        sendCount++;
        Thread.yield();
      }
      status = Status.MAP_FINISHED;
    }
  }

  private class BCastReceive implements MessageReceiver {
    private int count = 0;
    private Map<Integer, List<Integer>> receiveIds = new HashMap<>();
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));
        receiveIds.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      try {
        count++;
        if (count % 1 == 0) {
          LOG.info("Message received for last: " + source + " target: "
              + target + " count: " + count);
        }
        IntData data = (IntData) object;
        int sequence = data.getId();

        List<Integer> list = receiveIds.get(target);
        list.add(sequence);

        Set<Integer> r = new HashSet<>(list);
        if (list.size() != r.size()) {
          LOG.log(Level.INFO, "Duplicates", new RuntimeException("Dups"));
        }
      } catch (NullPointerException e) {
        LOG.log(Level.INFO, "Message received for last: " + source + " target: "
            + target + " count: " + count + " rids: " + receiveIds, e);
      }

      return true;
    }

    @Override
    public void progress() {
    }
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int[] d = new int[10];
    for (int i = 0; i < 10; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName("basic-broadcast")
        .setContainerClass(BaseBroadcastCommunication.class.getName())
        .setRequestResource(new ResourceContainer(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }
}

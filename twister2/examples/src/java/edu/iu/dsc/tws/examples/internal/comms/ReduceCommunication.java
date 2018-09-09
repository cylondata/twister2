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
package edu.iu.dsc.tws.examples.internal.comms;

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
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowReduce;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

/**
 * This will be a map-reduce job only using the communication primitives
 */
public class ReduceCommunication implements IWorker {
  private static final Logger LOG = Logger.getLogger(ReduceCommunication.class.getName());

  private DataFlowReduce reduce;

  private int id;

  private static final int NO_OF_TASKS = 8;

  @Override
  public void execute(Config cfg, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    LOG.log(Level.INFO, "Starting the example with container id: " + resources.getWorkerId());

    this.id = workerID;
    int noOfTasksPerExecutor = NO_OF_TASKS / resources.getNumberOfWorkers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, resources, NO_OF_TASKS);
    //first get the communication config file
    TWSChannel network = Network.initializeChannel(cfg, workerController, resources);

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    // this method calls the execute method
    // I think this is wrong
    reduce = new DataFlowReduce(network, sources, dest,
        new FinalReduceReceive(), new PartialReduceWorker());
    reduce.init(cfg, MessageType.OBJECT, taskPlan, 0);

    for (int i = 0; i < noOfTasksPerExecutor; i++) {
      // the map thread where data is produced
      LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
      Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
      mapThread.start();
    }
    // we need to communicationProgress the communication
    while (true) {
      try {
        // communicationProgress the channel
        network.progress();
        // we should communicationProgress the communication directive
        reduce.progress();
        Thread.yield();
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int task = 0;
    private int sendCount = 0;
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
//      MPIBuffer data = new MPIBuffer(1024);
        IntData data = generateData();
        for (int i = 0; i < 100; i++) { //original 11000
          // lets generate a message
          while (!reduce.send(task, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
//          LOG.info(String.format("%d sending to %d", id, task)
//              + " count: " + sendCount++);
          if (i % 10 == 0) {
            LOG.info(String.format("%d sent %d", id, i));
          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * Reduce class will work on the reduce messages.
   */
  private class PartialReduceWorker implements MessageReceiver {

    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();

    private int count = 0;
    /**
     * For each task in this exector, we will receive from the list of tasks in the given path
     *
     * @param expectedIds expected task ids
     */
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();
        Map<Integer, Integer> totalCountsPerTask = new HashMap<>();


        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
          totalCountsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Partial Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
        totalCounts.put(e.getKey(), totalCountsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d Message received for partial %d from %d", id, target, source));
      // add the object to the map
      boolean canAdd = true;
      try {
        List<Object> m = messages.get(target).get(source);
        Integer c = counts.get(target).get(source);
        if (m.size() > 128) {
//          if (count % 10 == 0) {
//          LOG.info(String.format("%d Partial false %d %d", id, source, m.size()));
//          }
          canAdd = false;
        } else {
//          if (count % 10 == 0) {
//          }
          m.add(object);
          counts.get(target).put(source, c + 1);
          Integer tc = totalCounts.get(target).get(source);
          totalCounts.get(target).put(source, tc + 1);
//          LOG.info(String.format("%d Partial true %d %d %s", id, source, m.size(), counts));
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public boolean progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
          Map<Integer, Integer> cMap = counts.get(t);
          boolean found = true;
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            } else {
              o = e.getValue().get(0);
            }
          }
          if (found) {
            if (o != null) {
              if (reduce.sendPartial(t, o, 0)) {
                count++;
                for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
                  o = e.getValue().remove(0);
                }
                for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
                  Integer i = e.getValue();
                  cMap.put(e.getKey(), i - 1);
                }
//                LOG.info(String.format("%d reduce send true", id));
              } else {
                canProgress = false;
//                LOG.info(String.format("%d reduce send false", id));
              }
              if (count % 10 == 0) {
                LOG.info(String.format("%d Inject partial %d count: %d %s",
                    id, t, count, counts));
              }
            } else {
              canProgress = false;
              LOG.severe("We cannot find an object and this is not correct");
            }
          }
        }
      }
      return true;
    }
  }

  private class FinalReduceReceive implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      // add the object to the map
      boolean canAdd = true;
      if (count == 0) {
        start = System.nanoTime();
      }

      try {
        List<Object> m = messages.get(target).get(source);
        Integer c = counts.get(target).get(source);
        if (m.size() > 128) {
          canAdd = false;
        } else {
          m.add(object);
          counts.get(target).put(source, c + 1);
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public boolean progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
          boolean found = true;
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            } else {
              o = e.getValue().get(0);
            }
          }
          if (found) {
            for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
              o = e.getValue().remove(0);
            }
            if (o != null) {
              count++;
              if (count % 10 == 0) {
                LOG.info(String.format("%d Last %d count: %d %s",
                    id, t, count, counts));
              }
              if (count >= 11000) {
                LOG.info("Total time: " + (System.nanoTime() - start) / 1000000
                    + " Count: " + count);
              }
            } else {
              LOG.severe("We cannot find an object and this is not correct");
            }
          }
        }
      }
      return true;
    }
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int s = 64000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName("basic-reduce")
        .setWorkerClass(ReduceCommunication.class.getName())
        .setRequestResource(new WorkerComputeResource(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

}

package edu.iu.dsc.tws.examples;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.Operation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.DefaultMessageReceiver;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

/**
 * This will be a map-reduce job only using the communication primitives
 */
public class BaseReduceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseReduceCommunication.class.getName());

  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private BlockingQueue<Message> partialReceiveQueue = new ArrayBlockingQueue<Message>(1024);

  private BlockingQueue<Message> reduceReceiveQueue = new ArrayBlockingQueue<Message>(1024);

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;

    // lets create the task plan
    TaskPlan taskPlan = createTaskPlan(cfg, plan);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    Map<String, Object> newCfg = new HashMap<>();

    // this method calls the init method
    // I think this is wrong
    reduce = channel.setUpDataFlowOperation(Operation.REDUCE, containerId, sources,
        dests, newCfg, 0, new DefaultMessageReceiver(reduceReceiveQueue),
        new MPIMessageDeSerializer(), new MPIMessageSerializer(),
        new DefaultMessageReceiver(partialReceiveQueue));

    // this thread is only run at the reduce
    Thread reduceThread = new Thread(new PartialReduceWorker());

    // the map thread where data is produced
    Thread mapThread = new Thread(new MapWorker());

    reduceThread.start();
    mapThread.start();

    try {
      mapThread.join();
      reduceThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to wait on threads");
    }

  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    @Override
    public void run() {
      for (int i = 0; i < 100000; i++) {
        Message message = Message.newBuilder().setPayload(generateData()).build();



        // lets generate a message
        reduce.sendCompleteMessage(message);
      }
    }
  }

  /**
   * Reduce class will work on the reduce messages.
   */
  private class PartialReduceWorker implements Runnable {
    @Override
    public void run() {
      while (true) {
        Message message = partialReceiveQueue.poll();
        Object payload = message.getPayload();

      }
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

  /**
   * Let assume we have 1 task per container
   * @param plan the resource plan from scheduler
   * @return task plan
   */
  private TaskPlan createTaskPlan(Config cfg, ResourcePlan plan) {
    int noOfProcs = plan.noOfContainers();

    Map<Integer, Set<Integer>> executorToChannels = null;
    Map<Integer, Set<Integer>> groupsToChannels = null;
    int thisExecutor = 0;
    int thisTaskk = 0;

    TaskPlan taskPlan = new TaskPlan(executorToChannels, groupsToChannels, thisExecutor, thisTaskk);
    return taskPlan;
  }
}

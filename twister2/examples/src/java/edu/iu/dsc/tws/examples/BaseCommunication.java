package edu.iu.dsc.tws.examples;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.Operation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

/**
 * This will be a map-reduce job only using the communication primitives
 */
public class BaseCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseCommunication.class.getName());

  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    this.config = config;
    this.resourcePlan = resourcePlan;
    this.id = id;

    // lets create the task plan
    TaskPlan taskPlan = null;
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(config, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    Map<String, Object> cfg = new HashMap<>();

    // this method calls the init method
    // I think this is wrong
    reduce = channel.setUpDataFlowOperation(Operation.REDUCE, id, sources,
        dests, cfg, 0, new ReduceMessageReceiver(),
        new ReduceMessageDeSerializer(), new ReduceMessageSerializer());

    Thread reduceThread = new Thread(new ReduceWorker());
    Thread mapThread = new Thread(new MapWorker());
  }

  private class MapWorker implements Runnable {
    @Override
    public void run() {
      Message.Builder messageBuilder = Message.newBuilder();
      MessageHeader.Builder messageHeaderBuilder = MessageHeader.newBuilder(id, id + 1, 0, 100, id);
      for (int i = 0; i < 1000; i++) {
        reduce.sendComplete(null);
      }
    }
  }

  private class ReduceWorker implements Runnable {
    @Override
    public void run() {

    }
  }

  private class ReduceMessageReceiver implements MessageReceiver {
    @Override
    public void init(Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public void onMessage(Object object) {

    }
  }

  private class ReduceMessageDeSerializer implements MessageDeSerializer {
    @Override
    public Object buid(Object message) {
      if (message instanceof MPIMessage) {
        // now deserialize it
      }
      return null;
    }
  }

  private class ReduceMessageSerializer implements MessageSerializer {

    @Override
    public Object build(Message message) {
      return null;
    }
  }
}

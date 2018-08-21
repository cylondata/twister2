
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
package edu.iu.dsc.tws.examples.internal.task.checkpoint;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.common.resource.WorkerComputeSpec;
import edu.iu.dsc.tws.common.resource.ZResourcePlan;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceCheckpointableTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

public class SourceSinkDiscoveryExample implements IContainer {

  private static final Logger LOG = Logger.getLogger(SourceSinkDiscoveryExample.class.getName());

  @Override
  public void init(Config config, int id, ZResourcePlan resourcePlan) {
    GeneratorTask g
        = new GeneratorTask();
    ReceivingTask r
        = new ReceivingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 4);
    builder.connect("source", "sink", "partition-edge", Operations.PARTITION);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));


    DataFlowTaskGraph graph = builder.build();

    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    TWSNetwork network = new TWSNetwork(config, resourcePlan.getThisId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resourcePlan, network);
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, plan, network.getChannel());
    executor.execute();
  }

  private static class GeneratorTask extends SourceCheckpointableTask {
    private static final long serialVersionUID = -254264903510214748L;

    @Override
    public void run() {
      ctx.write("partition-edge", "Hello");
    }

  }


  private static class ReceivingTask extends SinkTask {
    private static final long serialVersionUID = -254264903511284798L;
    private Config config;

    private RRClient client;
    private Progress looper;

    private TaskContext ctx;

    @Override
    public boolean execute(IMessage message) {
      System.out.println(message.getContent());
      return true;
    }

    public boolean tryUntilConnected(long timeLimit) {
      long startTime = System.currentTimeMillis();
      long duration = 0;
      long sleepInterval = 30;

      long logInterval = 1000;
      long nextLogTime = logInterval;

      while (duration < timeLimit) {
        // try connecting
        client.connect();
        // loop once to connect
        looper.loop();

        if (client.isConnected()) {
          return true;
        }


        if (client.isConnected()) {
          return true;
        }

        duration = System.currentTimeMillis() - startTime;

        if (duration > nextLogTime) {
          LOG.info("Still trying to connect to Job Master");
          nextLogTime += logInterval;
        }
      }
      return false;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      LOG.info("This is sample message............");

      looper = new Progress();

      client = new RRClient("localhost", 6789, cfg, looper,
          context.taskId(), new ClientConnectHandler());

      client.registerResponseHandler(Checkpoint.TaskDiscovery.newBuilder(),
          new ClientMessageHandler());

      long connectionTimeLimit = 100000;
      tryUntilConnected(connectionTimeLimit);

    }


    private class ClientConnectHandler implements ConnectHandler {
      @Override
      public void onError(SocketChannel channel) {
        LOG.severe("ClientConnectHandler error thrown inside Sink Task");
      }

      @Override
      public void onConnect(SocketChannel channel, StatusCode status) {
        LOG.info("ClientConnectHandler connected inside Sink Task");
        Checkpoint.TaskDiscovery message = Checkpoint.TaskDiscovery.newBuilder()
            .setTaskID(ctx.taskId())
            .setTaskType(Checkpoint.TaskDiscovery.TaskType.SINK)
            .build();

        client.sendRequest(message);
      }

      @Override
      public void onClose(SocketChannel channel) {

      }
    }

    private class ClientMessageHandler implements MessageHandler {
      @Override
      public void onMessage(RequestID id, int workerId, Message message) {
        LOG.info("ClientMessageHandler inside sink task got message from worker ID "
            + workerId);

        client.disconnect();
      }
    }
  }

  public WorkerPlan createWorkerPlan(ZResourcePlan resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeSpec resource : resourcePlan.getContainers()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("source-sink-discovery-example");
    jobBuilder.setWorkerClass(SourceSinkDiscoveryExample.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeSpec(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }
}


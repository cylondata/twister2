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
package edu.iu.dsc.tws.examples.task.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class IterativeJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(IterativeJob.class.getName());

  @Override
  public void execute(Config config, int workerId, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerId, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    IterativeSourceTask g = new IterativeSourceTask();
    PartitionTask r = new PartitionTask();

    ComputeGraphBuilder graphBuilder = ComputeGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", g, 4);
    ComputeConnection computeConnection = graphBuilder.addCompute("sink", r, 4);
    computeConnection.partition("source")
        .viaEdge("partition")
        .withDataType(MessageTypes.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    ComputeGraph graph = graphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    IExecutor ex = taskExecutor.createExecution(graph, plan);
    for (int i = 0; i < 10; i++) {
      LOG.info("Starting iteration: " + i);
      // this is a blocking call
      ex.execute();
    }
    ex.closeExecution();
  }

  private static class IterativeSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataObjectImpl<Object> input;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition", "Hello")) {
          count++;
        }
      }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void add(String name, DataObject<?> data) {
      input = (DataObjectImpl<Object>) data;
    }

    @Override
    public void reset() {
      count = 0;
    }
  }

  private static class PartitionTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private List<String> list = new ArrayList<>();

    private int count;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          count++;
          list.add(ret.toString());
        }
      }
      LOG.info("RECEIVE Count: " + count);
      return true;
    }

    @Override
    public void reset() {
      count = 0;
    }

    @Override
    public DataPartition<Object> get() {
      return new EntityPartition<>(context.taskIndex(), list);
    }
  }

  public static void main(String[] args) {
    LOG.log(Level.INFO, "Iterative job");
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("iterative-job");
    jobBuilder.setWorkerClass(IterativeJob.class.getName());
    jobBuilder.addComputeResource(4, 1024, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}

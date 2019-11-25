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
package edu.iu.dsc.tws.examples.checkpointing;

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;

public class CheckpointingTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(CheckpointingTaskExample.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    ComputeEnvironment computeEnvironment = ComputeEnvironment.init(
        config, workerID, workerController, persistentVolume, volatileVolume);

    ComputeGraphBuilder computeGraphBuilder =
        computeEnvironment.newTaskGraph(OperationMode.STREAMING);

    int parallelism = config.getIntegerValue("parallelism", 1);

    computeGraphBuilder.addSource("source", new SourceTask(), parallelism);

    computeGraphBuilder.addCompute("compute", new ComputeTask(), parallelism)
        .direct("source").viaEdge("so-c").withDataType(MessageTypes.INTEGER);

    computeGraphBuilder.addCompute("sink", new SinkTask(), parallelism)
        .direct("compute")
        .viaEdge("c-si")
        .withDataType(MessageTypes.INTEGER);

    computeEnvironment.buildAndExecute(computeGraphBuilder);
    computeEnvironment.close();
  }

  public static class ComputeTask extends BaseCompute<Integer> implements CheckpointableTask {

    private int count = 0;

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      this.count = (int) snapshot.getOrDefault("count", 0);
      LOG.info("Restored compute to  " + count);
    }

    @Override
    public void takeSnapshot(Snapshot snapshot) {
      snapshot.setValue("count", this.count);
    }

    @Override
    public void initSnapshot(Snapshot snapshot) {
      snapshot.setPacker("count", MessageTypes.INTEGER.getDataPacker());
    }

    @Override
    public boolean execute(IMessage<Integer> content) {
      this.count = content.getContent();
      context.write("c-si", this.count);
      return true;
    }
  }

  public static class SinkTask extends BaseCompute<Integer> implements CheckpointableTask {

    private int count = 0;

    @Override
    public boolean execute(IMessage<Integer> content) {
      this.count = content.getContent();
      return true;
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      this.count = (int) snapshot.getOrDefault("count", 0);
      LOG.info("Restored sinks to  " + count);
    }

    @Override
    public void takeSnapshot(Snapshot snapshot) {
      snapshot.setValue("count", this.count);
    }

    @Override
    public void initSnapshot(Snapshot snapshot) {
      snapshot.setPacker("count", MessageTypes.INTEGER.getDataPacker());
    }
  }


  public static class SourceTask extends BaseSource implements CheckpointableTask {

    private int count = 0;

    @Override
    public void execute() {
      context.write("so-c", count++);
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      this.count = (int) snapshot.getOrDefault("count", 0);
      LOG.info("Restored source to  " + count);
    }

    @Override
    public void takeSnapshot(Snapshot snapshot) {
      snapshot.setValue("count", this.count);
    }

    @Override
    public void initSnapshot(Snapshot snapshot) {
      snapshot.setPacker("count", MessageTypes.INTEGER.getDataPacker());
    }
  }

  public static void main(String[] args) {
    int numberOfWorkers = 4;
    if (args.length == 1) {
      numberOfWorkers = Integer.valueOf(args[0]);
    }

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("parallelism", numberOfWorkers);


    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("hello-checkpointing-job")
        .setWorkerClass(CheckpointingTaskExample.class)
        .addComputeResource(1, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}

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
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskEnvironment;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.checkpointing.api.Snapshot;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class CheckpointingTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(CheckpointingTaskExample.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    TaskEnvironment taskEnvironment = TaskEnvironment.init(
        config, workerID, workerController, volatileVolume);

    TaskGraphBuilder taskGraphBuilder = taskEnvironment.newTaskGraph(OperationMode.STREAMING);

    int parallelism = config.getIntegerValue("parallelism", 1);

    taskGraphBuilder.addSource("source", new SourceTask(), parallelism);

    taskGraphBuilder.addSink("sink", new Sinktask(), parallelism).direct(
        "source",
        "edge",
        DataType.INTEGER
    );

    taskEnvironment.buildAndExecute(taskGraphBuilder);
    taskEnvironment.close();
  }

  public static class Sinktask extends BaseSink<Integer> implements CheckpointableTask {

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
      context.write("edge", count++);
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

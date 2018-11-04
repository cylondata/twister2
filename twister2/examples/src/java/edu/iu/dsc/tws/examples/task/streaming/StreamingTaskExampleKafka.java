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
package edu.iu.dsc.tws.examples.task.streaming;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.checkpointmanager.state_backend.FsCheckpointStorage;
import edu.iu.dsc.tws.checkpointmanager.utils.CheckpointContext;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.connectors.TwsKafkaConsumer;
import edu.iu.dsc.tws.connectors.TwsKafkaProducer;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.local.LocalFileSystem;
import edu.iu.dsc.tws.examples.internal.task.TaskUtils;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.core.Runtime;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.SinkCheckpointableTask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class StreamingTaskExampleKafka implements IWorker {
  @Override
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    Path path = new Path(new File(CheckpointContext
        .getStatebackendDirectoryDefault(config)).toURI());
    path.getParent();

    Runtime runtime = new Runtime();
    runtime.setParentpath(path);

    LocalFileSystem localFileSystem = new LocalFileSystem();
    runtime.setFileSystem(localFileSystem);
    Config newconfig = runtime.updateConfig(config);

    if (workerID == 1) {
//      LOG.log(Level.INFO, "Statebackend directory is created for job: " + runtime.getJobName());
      FsCheckpointStorage newStateBackend = new FsCheckpointStorage(localFileSystem, path,
          runtime.getJobName(), 0);
    }

    List<String> topics = new ArrayList<>();
    topics.add("sample_topic1");
    List<String> servers = new ArrayList<>();
    servers.add("localhost:9092");
    TwsKafkaConsumer<String> g = new TwsKafkaConsumer<String>(
        topics,
        servers,
        "test",
        "partition-edge");
    TwsKafkaProducer<String> r = new TwsKafkaProducer<String>(
        "outTopic",
        servers
    );

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 4);
    builder.connect("source", "sink", "partition-edge",
        OperationNames.PARTITION);
    builder.operationMode(OperationMode.STREAMING);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(newconfig));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(newconfig));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(newconfig));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("source", "inputdataset", sourceInputDataset);

    DataFlowTaskGraph graph = builder.build();

    TaskUtils.execute(newconfig, resources, graph, workerController);
  }

  private static class RecevingTask extends SinkCheckpointableTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 1000000 == 0) {
        System.out.println(message.getContent());
      }
      count++;
      return true;
    }

    @Override
    public void addCheckpointableStates() {
      this.addState("trial", 2);
    }
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
    jobBuilder.setName("task-example");
    jobBuilder.setWorkerClass(StreamingTaskExampleKafka.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(1, 512), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}

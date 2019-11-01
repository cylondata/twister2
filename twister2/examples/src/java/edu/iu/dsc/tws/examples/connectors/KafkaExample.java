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
package edu.iu.dsc.tws.examples.connectors;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.comms.packing.types.primitive.LongPacker;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.IComputableSink;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.connectors.kafka.KafkaSource;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;

public class KafkaExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(KafkaSource.class.getName());

  public static void main(String[] args) {

    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(KafkaExample.class.getName())
        .setWorkerClass(KafkaExample.class)
        .addComputeResource(1, 1024, 1)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  public static class KSource extends KafkaSource {

    @Override
    public Properties getConsumerProperties() {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringDeserializer");
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringDeserializer");
      return props;
    }

    @Override
    public Set<String> getTopics() {
      Set<String> topics = new HashSet<>();
      topics.add("test2");
      return topics;
    }

    @Override
    public void writeRecord(ConsumerRecord kafkaRecord) {
      this.context.write("edge", kafkaRecord.value());
    }

    @Override
    public Duration getPollingTimeout() {
      return Duration.ZERO;
    }
  }

  public static class KSink implements IComputableSink, CheckpointableTask {

    private Long sum = 0L;
    private TaskContext context;

    @Override
    public boolean execute(IMessage content) {
      sum += Long.parseLong(content.getContent().toString());
      LOG.info("Current sum in " + context.taskIndex() + " : " + sum);
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {

      this.context = ctx;
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      sum = (Long) snapshot.getOrDefault("sum", 0L);
      LOG.info("Restoring sum in " + this.context.taskIndex() + " : " + sum);
    }

    @Override
    public void takeSnapshot(Snapshot snapshot) {
      snapshot.setValue("sum", sum);
    }

    @Override
    public void initSnapshot(Snapshot snapshot) {
      snapshot.setPacker("sum", LongPacker.getInstance());
    }
  }

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);

    ComputeGraphBuilder graphBuilder = ComputeGraphBuilder.newBuilder(config);
    graphBuilder.setMode(OperationMode.STREAMING);

    graphBuilder.addSource("ksource", new KSource(), 2);
    graphBuilder.addSink("sink", new KSink(), 2)
        .direct("ksource").viaEdge("edge");

    cEnv.buildAndExecute(graphBuilder);
  }
}

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
  private static final String CLI_TOPICS = "topics";
  private static final String CLI_SERVER = "server";

  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(CLI_SERVER, true,
        "Kafka bootstrap server in the format host:port");
    options.addOption(CLI_TOPICS, true,
        "Set of topics in the format topic1,topic2");

    CommandLineParser cliParser = new DefaultParser();
    CommandLine cli = cliParser.parse(options, args);

    HashMap<String, Object> configs = new HashMap<>();
    configs.put(CLI_SERVER, "localhost:9092");
    configs.put(CLI_TOPICS, Collections.singleton("test2"));

    if (cli.hasOption(CLI_SERVER)) {
      configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cli.getOptionValue(CLI_SERVER));
    }

    if (cli.hasOption(CLI_TOPICS)) {
      String topics = cli.getOptionValue(CLI_TOPICS);
      Set<String> topicsSet = Arrays.stream(topics.split(","))
          .map(String::trim).collect(Collectors.toSet());
      configs.put(CLI_TOPICS, topicsSet);
    }

    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configs);

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
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getStringValue(CLI_SERVER));
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringDeserializer");
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringDeserializer");
      return props;
    }

    @Override
    public Set<String> getTopics() {
      return cfg.get(CLI_TOPICS, Set.class);
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
    graphBuilder.addCompute("sink", new KSink(), 2)
        .direct("ksource").viaEdge("edge");

    cEnv.buildAndExecute(graphBuilder);
  }
}


package edu.iu.dsc.tws.connectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;

public class TwsKafkaConsumer<T> extends SourceTask {
  private static final long serialVersionUID = -264264120110286748L;
  private static final Logger LOG = LoggerFactory.getLogger(TwsKafkaConsumer.class);

  private Properties kafkaConfigs;
  private List<TopicPartition> topicPartitions;
  private int myIndex;
  private int worldSize;
  private TaskContext taskContext;
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
  private List<KafkaTopicPartitionState> topicPartitionStates;

  private boolean restoreState = false;
  private volatile boolean consumerThreadStarted = false;

  private  KafkaPartitionFinder partitionFinder;
  private  KafkaTopicDescription topicDescription;
  private  KafkaConsumerThread<T> kafkaConsumerThread;

  @Override
  public void run() {
    System.out.println("start");
    if (!consumerThreadStarted) {
      try {
        kafkaConsumerThread.start();

      } catch (IllegalThreadStateException e) {
        LOG.info("consumer is already started");
      }
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {

  }
  public TwsKafkaConsumer(
      List<String> topics,
      List<String> servers,
      String consumerGroup
  ) {
    this.topicDescription = new KafkaTopicDescription(topics);
    createKafkaConfig(servers, consumerGroup);
  }

  public TwsKafkaConsumer(
      Pattern topicPattern,
      List<String> servers,
      String consumerGroup
  ) {
    this.topicDescription = new KafkaTopicDescription(topicPattern);
    createKafkaConfig(servers, consumerGroup);
  }

  public Properties getKafkaConfigs() {
    return kafkaConfigs;
  }

  public void setKafkaConfigs(Properties kafkaConfigs) {
    this.kafkaConfigs = kafkaConfigs;
  }

  //    public void prepare(Config cfg, TaskContext context) {
//        this.
//    }
  public void init() {
    System.out.println("init");
    this.worldSize = 1;
    this.myIndex = 0;
    this.partitionFinder = new KafkaPartitionFinder(
        this.kafkaConfigs, worldSize, myIndex, topicDescription);
    this.topicPartitions = partitionFinder.getRelevantPartitions();

    this.topicPartitionStates = new ArrayList<>();
    for (TopicPartition tp : topicPartitions) {
      topicPartitionStates.add(new KafkaTopicPartitionState(tp));
    }

    this.kafkaConsumerThread = new KafkaConsumerThread<T>(
        kafkaConfigs, offsetsToCommit, topicPartitions, topicPartitionStates);
    kafkaConsumerThread.assignPartitions();
    kafkaConsumerThread.setSeekToBeginning();

  }

//  public void run(){
//
//  }

  private Properties createKafkaConfig(List<String> servers, String consumerGroup) {

    StringBuilder strServers = new StringBuilder();
    for (int i = 0; i < servers.size(); i++) {
      strServers.append(servers.get(i));
      if (i + 1 < servers.size()) {
        strServers.append(",");
      }
    }
    this.kafkaConfigs = new Properties();
    kafkaConfigs.put("bootstrap.servers", strServers.toString());
    kafkaConfigs.put("group.id", consumerGroup);
    kafkaConfigs.put("enable.auto.commit", "false");
    kafkaConfigs.put("session.timeout.ms", "30000");
    kafkaConfigs.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaConfigs.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    return kafkaConfigs;
  }

}

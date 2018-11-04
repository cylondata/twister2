
package edu.iu.dsc.tws.connectors;


import java.util.ArrayList;
//import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
//import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.connectors.config.KafkaConsumerConfig;
//import edu.iu.dsc.tws.task.api.Snapshot;
import edu.iu.dsc.tws.connectors.config.KafkaTopicPartitionsWrapper;
import edu.iu.dsc.tws.connectors.config.PartitionState;
import edu.iu.dsc.tws.task.api.Snapshot;
import edu.iu.dsc.tws.task.api.SourceCheckpointableTask;
import edu.iu.dsc.tws.task.api.TaskContext;

public class TwsKafkaConsumer<T> extends SourceCheckpointableTask {
  private static final long serialVersionUID = -264264120110286748L;
  private static Logger log = LoggerFactory.getLogger(TwsKafkaConsumer.class);

  private Properties kafkaConfigs;
  private Properties simpleKafkaConfig;
  private List<TopicPartition> topicPartitions;
  private int myIndex;
  private int worldSize;
  private TaskContext taskContext;
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
  private ArrayList<KafkaTopicPartitionState> topicPartitionStates;
  private ArrayList<PartitionState> state = new ArrayList<>();
  private String edge;

  private boolean restoreState = false;
  private volatile boolean consumerThreadStarted = false;

  private KafkaPartitionFinder partitionFinder;
  private KafkaTopicDescription topicDescription;
  private KafkaConsumerThread<T> kafkaConsumerThread;


  @Override
  public void execute() {
    try {
      kafkaConsumerThread.run();
    } catch (IllegalThreadStateException e) {
      log.info(e.toString());
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
//    connect(cfg, context);
//    setCheckpointInterval(4);

    if (!restoreState) {
      this.myIndex = cfg.getIntegerValue("twister2.container.id", 0);
      this.worldSize = context.getParallelism();
      log.info("myID : {} , worldSize : {} ", myIndex, worldSize);
      this.partitionFinder = new KafkaPartitionFinder(
          simpleKafkaConfig, worldSize, myIndex, topicDescription);
      this.topicPartitions = partitionFinder.getRelevantPartitions();

      this.topicPartitionStates = new ArrayList<>();
      for (TopicPartition tp : topicPartitions) {
        topicPartitionStates.add(new KafkaTopicPartitionState(tp));
      }
      this.offsetsToCommit = new HashMap<>();
      this.kafkaConsumerThread = new KafkaConsumerThread<T>(
          kafkaConfigs, offsetsToCommit, topicPartitions, topicPartitionStates, context, edge);
      kafkaConsumerThread.assignPartitions();
      log.info("{} partitions are assigned", this.topicPartitions.size());
      kafkaConsumerThread.setSeekToEnd();
    } else {

      this.myIndex = cfg.getIntegerValue("twister2.container.id", 0);
      this.worldSize = context.getParallelism();
      log.info("myID : {} , worldSize : {} ", myIndex, worldSize);
      this.offsetsToCommit = new HashMap<>();
      this.kafkaConsumerThread = new KafkaConsumerThread<T>(
          kafkaConfigs, offsetsToCommit, topicPartitions, topicPartitionStates, context, edge);
      kafkaConsumerThread.assignPartitions();
      log.info("{} partitions are assigned", this.topicPartitions.size());
      kafkaConsumerThread.setSeekToRestore();
    }

  }

  public TwsKafkaConsumer(
      List<String> topics,
      List<String> servers,
      String consumerGroup,
      String edge
  ) {
    this.topicDescription = new KafkaTopicDescription(topics);
    this.kafkaConfigs = createKafkaConfig(servers, consumerGroup);
    this.simpleKafkaConfig = createSimpleKafkaConfig(servers);
    this.edge = edge;
  }

  public TwsKafkaConsumer(
      Pattern topicPattern,
      List<String> servers,
      String consumerGroup,
      String edge
  ) {
    this.topicDescription = new KafkaTopicDescription(topicPattern);
    this.kafkaConfigs = createKafkaConfig(servers, consumerGroup);
    this.simpleKafkaConfig = createSimpleKafkaConfig(servers);
    this.edge = edge;
  }

  public Properties getKafkaConfigs() {
    return kafkaConfigs;
  }

  public void setKafkaConfigs(Properties kafkaConfigs) {
    this.kafkaConfigs = kafkaConfigs;
  }

  private Properties createKafkaConfig(List<String> servers, String consumerGroup) {
    return KafkaConsumerConfig.getStringDeserializerConfig(servers, consumerGroup);
  }

  private Properties createSimpleKafkaConfig(List<String> servers) {
    return KafkaConsumerConfig.getSimpleKafkaConsumer(servers);
  }

  public TwsKafkaConsumer() {
  }

  public void saveCheckpoint() {

  }

  @Override
  public void addCheckpointableStates() {
    this.addState("kafkaConsumer", new KafkaTopicPartitionsWrapper(state));
  }

  @Override
  public void restoreSnapshot(Snapshot newsnapshot) {
    super.restoreSnapshot(newsnapshot);
    restoreState = true;
    Object rawState = this.getState("kafkaConsumer");
    HashSet<String> uniqueTopicPartition = new HashSet<>();
    if (rawState instanceof KafkaTopicPartitionsWrapper) {
      ArrayList<KafkaTopicPartitionState> kafkaState = new ArrayList<>();
      ArrayList<TopicPartition> newTopicPartitions = new ArrayList<>();
      ArrayList<PartitionState> newStates = ((KafkaTopicPartitionsWrapper) rawState)
          .getTopicPartitionStates();
      this.state = newStates;

      for (PartitionState newState : newStates) {

        if (!uniqueTopicPartition.contains(newState.getTopic() + newState.getPartition())) {

          TopicPartition partition = new TopicPartition(
              newState.getTopic(),
              newState.getPartition());
          KafkaTopicPartitionState topicPartitionState = new KafkaTopicPartitionState(partition);
          topicPartitionState.setCommitOffset(newState.getCommitOffset());
          topicPartitionState.setPositionOffset(newState.getPositionOffset());
          kafkaState.add(topicPartitionState);
          newTopicPartitions.add(partition);
          uniqueTopicPartition.add(newState.getTopic() + newState.getPartition());

          log.info("******Restoring Status********");
          log.info("Topic :" + newState.getTopic());
          log.info("partition :" + newState.getPartition());
          log.info("PositionOffset :" + newState.getPositionOffset());
          log.info("CommitOffset :" + newState.getCommitOffset());
        } else {
          for (KafkaTopicPartitionState partitionsState : kafkaState) {
            if ((partitionsState.getTopicPartition().topic()).equals(newState.getTopic())
                && partitionsState.getTopicPartition().partition() == newState.getPartition()) {
              partitionsState.setPositionOffset(newState.getPositionOffset());
              partitionsState.setCommitOffset(newState.getCommitOffset());
              log.info("******* Updating the State ******* : " + newState.getPositionOffset()
                  + " " + newState.getCommitOffset());
            }
          }
        }
      }
      this.topicPartitionStates = kafkaState;
      this.topicPartitions = newTopicPartitions;
      log.info("size: " + topicPartitionStates.size());
    }
  }

  @Override
  public void barrierEmitted() {
    for (KafkaTopicPartitionState kafkaTopicPartitionState : topicPartitionStates) {
      state.add(new PartitionState(
          kafkaTopicPartitionState.getTopicPartition().topic(),
          kafkaTopicPartitionState.getTopicPartition().partition(),
          kafkaTopicPartitionState.getPositionOffset(),
          kafkaTopicPartitionState.getCommitOffset()));

      log.info("Updating Status");
      log.info("Topic :" + kafkaTopicPartitionState.getTopicPartition().topic());
      log.info("partition :" + kafkaTopicPartitionState.getTopicPartition().partition());
      log.info("PositionOffset :" + kafkaTopicPartitionState.getPositionOffset());
      log.info("CommitOffset :" + kafkaTopicPartitionState.getCommitOffset());
    }
  }
}

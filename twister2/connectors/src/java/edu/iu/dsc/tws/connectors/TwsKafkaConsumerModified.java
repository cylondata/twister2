
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

//import edu.iu.dsc.tws.checkpointmanager.CheckpointListener;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.connectors.config.KafkaConsumerConfig;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;
//import edu.iu.dsc.tws.task.api.streaming.CheckpointedFunction;

public class TwsKafkaConsumerModified<T> extends SourceTask {
//    implements CheckpointedFunction, CheckpointListener
  private static final long serialVersionUID = -264264120110286748L;
  private static final Logger LOG = LoggerFactory.getLogger(TwsKafkaConsumer.class);

  private Properties kafkaConfigs;
  private Properties simpleKafkaConfig;
  private List<TopicPartition> topicPartitions;
  private int myIndex;
  private int worldSize;
  private TaskContext taskContext;
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
  private List<KafkaTopicPartitionState> topicPartitionStates;
  private final String edge;

  private boolean restoreState = false;
  private volatile boolean consumerThreadStarted = false;

  private  KafkaPartitionFinder partitionFinder;
  private  KafkaTopicDescription topicDescription;
  private  KafkaConsumerThread<T> kafkaConsumerThread;

  @Override
  public void execute() {
    if (!consumerThreadStarted) {
      try {
        kafkaConsumerThread.run();
        LOG.info("Starting the consumer thread");
      } catch (IllegalThreadStateException e) {
        LOG.info("consumer is already started");
      }
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    this.myIndex = cfg.getIntegerValue("twister2.container.id", 0);
    this.worldSize = context.getParallelism();
    LOG.info("myID : {} , worldSize : {} ", myIndex, worldSize);
    this.partitionFinder = new KafkaPartitionFinder(
        simpleKafkaConfig, worldSize, myIndex, topicDescription);
    this.topicPartitions = partitionFinder.getRelevantPartitions();

    this.topicPartitionStates = new ArrayList<>();
    for (TopicPartition tp : topicPartitions) {
      topicPartitionStates.add(new KafkaTopicPartitionState(tp));
    }

    this.kafkaConsumerThread = new KafkaConsumerThread<T>(
        kafkaConfigs, offsetsToCommit, topicPartitions, topicPartitionStates, context, edge);
    kafkaConsumerThread.assignPartitions();
    LOG.info("{} partitions are assigned", this.topicPartitions.size());
    kafkaConsumerThread.setSeekToBeginning();

  }
  public TwsKafkaConsumerModified(
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

  public TwsKafkaConsumerModified(
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

//  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {

  }
}

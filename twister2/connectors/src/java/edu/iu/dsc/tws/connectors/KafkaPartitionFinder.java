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
package edu.iu.dsc.tws.connectors;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPartitionFinder {
  private static Logger log = LoggerFactory.getLogger(KafkaPartitionFinder.class);
  private String[] bootstrapServers;
  private int numRetries;
  private int worldSize;
  private int myIndex;
  private Consumer<?, ?> consumer;
  private Properties kafkaConsumerConfig;
  private KafkaTopicDescription topics;

  public KafkaPartitionFinder(
      Properties kafkaConsumerConfig,
      int worldSize,
      int myIndex,
      KafkaTopicDescription topics
  ) {
    this.bootstrapServers = kafkaConsumerConfig.getProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).split(",");
    this.numRetries = 10;
    this.myIndex = myIndex;
    this.worldSize = worldSize;
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.topics = topics;
    initializeConnection();
  }

  public List<TopicPartition> getAllPartitions() {
    if (this.topics.isFixedTopics()) {
      return getAllPartitionsForTopics(topics.getFixedTopics());
    } else {
      List<String> allTopics = allTopics();
      List<String> relevantTopics = new LinkedList<>();
      Pattern pattern = topics.getTopicPattern();
      for (String topic : allTopics) {
        Matcher matcher = pattern.matcher(topic);
        if (matcher.find()) {
          relevantTopics.add(topic);
        }
      }
      return getAllPartitionsForTopics(relevantTopics);

    }
  }

  public List<TopicPartition> getRelevantPartitions() {
    List<TopicPartition> relevantPartitions = new LinkedList<>();
    List<TopicPartition> allPartitions = getAllPartitions();
    for (TopicPartition partition : allPartitions) {
      if (assignPartition(partition)) {
        relevantPartitions.add(partition);
      }
    }
    log.info("{} Partitions found for Process-{}", relevantPartitions.size(), myIndex);
    return relevantPartitions;
  }

  public KafkaPartitionFinder initializeConnection() {
    this.consumer = new KafkaConsumer<>(kafkaConsumerConfig);
    return this;
  }

  public List<String> allTopics() throws WakeupException {
    try {
      return new LinkedList<>(consumer.listTopics().keySet());
    } catch (WakeupException e) {
      throw new WakeupException();
    }
  }

  protected List<TopicPartition> getAllPartitionsForTopics(List<String> topics2)
      throws WakeupException {
    List<TopicPartition> partitions = new LinkedList<>();

    try {
      for (String topic : topics2) {
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
          partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
      }
    } catch (org.apache.kafka.common.errors.WakeupException e) {
      throw new WakeupException();
    }
    return partitions;
  }

  protected void wakeupConnections() {
    if (this.consumer != null) {
      this.consumer.wakeup();
    }
  }

  protected void closeConnections() throws Exception {
    if (this.consumer != null) {
      this.consumer.close();

      // de-reference the consumer to avoid closing multiple times
      this.consumer = null;
    }
  }

  private boolean assignPartition(TopicPartition topicPartition) {
    int startIndxOfTopic = topicPartition.topic().hashCode();
    int partitionAssignIndex = (startIndxOfTopic + topicPartition.partition()) % worldSize;
    return partitionAssignIndex == myIndex;
  }

  public KafkaPartitionFinder() {
  }
}

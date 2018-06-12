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

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.iu.dsc.tws.task.api.TaskContext;

public class KafkaConsumerThread<T>  {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerThread.class);
  private Consumer<String, String> consumer;
  private Properties kafkaConsumerConfig;
  private volatile boolean isOffsetsToCommit = false;
  private volatile boolean isCommitCommenced = false;
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
  private Map<TopicPartition, Long> offsetsToSubscribe;
  private List<TopicPartition> topicPartitions;
  private List<KafkaTopicPartitionState> topicPartitionStates;
  private volatile boolean active = true;
  private volatile TaskContext taskContext;
  private String edge;
  private boolean fetchLoopStarted = false;

  public KafkaConsumerThread(
      Properties kafkaConsumerConfig, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
      List<TopicPartition> topicPartitions, List<KafkaTopicPartitionState> topicPartitionStates,
      TaskContext context, String edge) {
    this.kafkaConsumerConfig = kafkaConsumerConfig;
    this.offsetsToCommit = offsetsToCommit;
    this.topicPartitions = topicPartitions;
    this.topicPartitionStates = topicPartitionStates;
    this.taskContext = context;
    this.edge = edge;

  }


  public void run() {
    if (!fetchLoopStarted) {
      LOG.info("starting");
      initiateConnection();
      commitOffsets();
      if (!active) {
        return;
      }
      if (topicPartitions == null) {
        throw new Error("Topic Partition is not defined");
      }
      consumer.assign(topicPartitions);
      fetchLoopStarted = true;
    }
    ConsumerRecords<String, String> records;
    records = consumer.poll(100);
    if (records != null) {
      for (ConsumerRecord<String, String> record : records) {
//      }
        for (KafkaTopicPartitionState topicPartitionState : topicPartitionStates) {

          List<ConsumerRecord<String, String>> partitionRecords =
              records.records(topicPartitionState.getTopicPartition());


          for (ConsumerRecord<String, String> record2 : partitionRecords) {
            String value = record2.value();
//            LOG.info("record = {} ; offset = {} ;", value, record2.offset());
            emitRecord(value, topicPartitionState, record2.offset());
          }
        }
      }
    }
  }

  public void initiateConnection() {
    if (this.consumer == null) {
      this.consumer = new KafkaConsumer<>(kafkaConsumerConfig);
    }
  }

  public void commitOffsets() {
    synchronized (this) {
      if (!isOffsetsToCommit) {
        return;
      }
    }
    if (isCommitCommenced) {
      return;
    }
    isCommitCommenced = true;
    isOffsetsToCommit = false;
    consumer.commitAsync(offsetsToCommit, new OffsetCommitCallback() {
      @Override
      public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        isCommitCommenced = false;
      }
    });
  }

  public synchronized void setOffsetsToCommit(Map<TopicPartition,
      OffsetAndMetadata> offsetsToCommit) {
    this.offsetsToCommit = offsetsToCommit;
    this.isOffsetsToCommit = true;
  }

  public void setSeek() {
    initiateConnection();
    for (TopicPartition topicPartition : offsetsToSubscribe.keySet()) {
      consumer.seek(topicPartition, offsetsToSubscribe.get(topicPartition));
    }
  }

  public void setSeekToBeginning() {
    initiateConnection();
    consumer.seekToBeginning(topicPartitions);
  }

  public void assignPartitions() {
    initiateConnection();
    consumer.assign(topicPartitions);
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public void emitRecord(String value, KafkaTopicPartitionState tps, Long offset) {
    LOG.info("emitting record {} from the partition {}", value, offset);
    tps.setPositionOffset(offset);
    taskContext.write(this.edge, value);
  }

}

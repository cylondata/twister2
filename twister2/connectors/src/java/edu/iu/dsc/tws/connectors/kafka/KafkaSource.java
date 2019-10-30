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
package edu.iu.dsc.tws.connectors.kafka;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;

public abstract class KafkaSource<K, V> implements CheckpointableTask, ISource {

  private static final Logger LOG = Logger.getLogger(KafkaSource.class.getName());
  private static final String LAST_PARTITION_OFFSETS = "LAST_PARTITION_OFFSETS";

  private KafkaConsumer<K, V> kafkaConsumer;
  private Config cfg;
  protected TaskContext context;

  private HashMap<String, HashMap<Integer, Long>> partitionOffsets;

  @Override
  public void prepare(Config config, TaskContext ctx) {
    this.cfg = config;
    this.context = ctx;

    Properties consumerProperties = this.getConsumerProperties();

    // override consumer group. Using task name as the group id by default.
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, this.getConsumerGroup(context));

    // disable auto commit
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    this.kafkaConsumer.subscribe(this.getTopics());

    if (this.partitionOffsets == null) {
      this.partitionOffsets = new HashMap<>();
    }
    this.partitionOffsets.forEach((topic, v) -> {
      v.forEach((partition, offset) -> {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        this.kafkaConsumer.seek(topicPartition, offset);
      });
    });

    this.partitionOffsets = new HashMap<>();
  }

  /**
   * This method returns the consumer group for this consumer.
   * By default it returns the task name, which will effectively load balance the data from
   * kafka across the tasks of this type.
   * Users can override this method to change this behaviour
   *
   * @param context Instance of {@link TaskContext}
   */
  public String getConsumerGroup(TaskContext context) {
    return context.taskName();
  }

  public abstract Properties getConsumerProperties();

  public abstract Set<String> getTopics();

  public abstract void writeRecord(ConsumerRecord<K, V> kafkaRecord);

  @Override
  public void execute() {
    ConsumerRecords<K, V> newRecords = this.kafkaConsumer.poll(1);
    for (ConsumerRecord<K, V> newRecord : newRecords) {
      this.writeRecord(newRecord);
      LOG.info(String.format("%s : %d : %d",
          newRecord.topic(), newRecord.partition(), newRecord.offset()));
      this.partitionOffsets.computeIfAbsent(newRecord.topic(),
          topic -> new HashMap<>()).put(newRecord.partition(), newRecord.offset());
    }
  }

  @Override
  public void restoreSnapshot(Snapshot snapshot) {
    this.partitionOffsets = (HashMap<String, HashMap<Integer, Long>>) snapshot.get(
        LAST_PARTITION_OFFSETS);
  }

  @Override
  public void takeSnapshot(Snapshot snapshot) {
    snapshot.setValue(LAST_PARTITION_OFFSETS, partitionOffsets);
  }


  @Override
  public void onCheckpointPropagated(Snapshot snapshot) {
    // todo sync or async commit?
    this.kafkaConsumer.commitSync();
  }

  @Override
  public void initSnapshot(Snapshot snapshot) {
    //nothing to do here.
  }
}

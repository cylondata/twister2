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

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;

public abstract class KafkaSource<K, V> implements CheckpointableTask, ISource {

  private KafkaConsumer<K, V> kafkaConsumer;
  private Config cfg;
  protected TaskContext context;

  @Override
  public void prepare(Config config, TaskContext ctx) {
    this.cfg = config;
    this.context = ctx;

    Properties consumerProperties = this.getConsumerProperties();

    //disable auto commit
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
  }

  public abstract Properties getConsumerProperties();

  public abstract void writeRecord(ConsumerRecord<K, V> kafkaRecord);

  @Override
  public void execute() {
    ConsumerRecords<K, V> newRecords = this.kafkaConsumer.poll(1);
    for (ConsumerRecord<K, V> newRecord : newRecords) {
      this.writeRecord(newRecord);
    }
  }

  @Override
  public void restoreSnapshot(Snapshot snapshot) {

  }

  @Override
  public void takeSnapshot(Snapshot snapshot) {

  }


  @Override
  public void onCheckpointPropagated(Snapshot snapshot) {
    this.kafkaConsumer.commitSync();
  }

  @Override
  public void initSnapshot(Snapshot snapshot) {

  }
}

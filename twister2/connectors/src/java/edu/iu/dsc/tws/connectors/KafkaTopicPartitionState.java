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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaTopicPartitionState {
  private TopicPartition topicPartition;
  private long positionOffset;
  private long commitOffset;


  public KafkaTopicPartitionState(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
  }

  public long getPositionOffset() {
    return positionOffset;
  }

  public void setPositionOffset(long positionOffset) {
    this.positionOffset = positionOffset;
  }

  public long getCommitOffset() {
    return commitOffset;
  }

  public void setCommitOffset(long commitOffset) {
    this.commitOffset = commitOffset;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public OffsetAndMetadata getEmptyMetadataOffset() {
    return new OffsetAndMetadata(positionOffset);
  }

  public KafkaTopicPartitionState() {
  }
}

//  Copyright 2017 Twitter. All rights reserved.
//
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

/**
 * this is the description of the kafka partition.
 */
public class KafkaTopicPartition {

  private final String topic;
  private final int partitionNum;

  public KafkaTopicPartition(String topic, int partitionNum) {
    this.topic = topic;
    this.partitionNum = partitionNum;
  }


  public int getPartitionNum() {
    return partitionNum;
  }

  public String getTopic() {
    return topic;
  }


//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        }
//        if (o instanceof KafkaTopicPartition) {
//            KafkaTopicPartition compr = (KafkaTopicPartition) o;
//            return ((compr.getTopic()).
//            equals(this.topic) && compr.getPartitionNum() == this.partitionNum);
//
//        }
//        else {
//            return false;
//        }
//    }
}

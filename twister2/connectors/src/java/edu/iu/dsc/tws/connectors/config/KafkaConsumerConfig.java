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
package edu.iu.dsc.tws.connectors.config;

import java.util.List;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerConfig;

public abstract class KafkaConsumerConfig {

  public static Properties getStringDeserializerConfig(List<String> servers, String consumerGroup) {
    String tempGroup = consumerGroup;
    if (tempGroup == null) {
      tempGroup = "TwsKafkaConsumer";
    }
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getStrServers(servers));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, tempGroup);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }

  public static Properties getLongDeserializerConfig(List<String> servers, String consumerGroup) {
    String tempGroup = consumerGroup;
    if (tempGroup == null) {
      tempGroup = "TwsKafkaConsumer";
    }
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getStrServers(servers));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, tempGroup);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.LongDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.LongDeserializer");
    return props;

  }
  private static String getStrServers(List<String> servers) {
    StringBuilder strServers = new StringBuilder();
    for (int i = 0; i < servers.size(); i++) {
      strServers.append(servers.get(i));
      if (i + 1 < servers.size()) {
        strServers.append(",");
      }
    }
    return strServers.toString();
  }

  public static Properties getSimpleKafkaConsumer(List<String> servers) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getStrServers(servers));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "partitionFinding");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.LongDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.LongDeserializer");
    return props;
  }

}

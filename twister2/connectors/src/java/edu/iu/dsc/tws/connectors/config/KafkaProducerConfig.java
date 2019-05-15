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

import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

public abstract class KafkaProducerConfig {
  public static Properties getConfig(List<String> servers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getStrServers(servers));
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 3355432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
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

  public static Properties setProps(Properties conf, Properties newProps) {
    Enumeration<?> keys = newProps.propertyNames();
    while (keys.hasMoreElements()) {
      if (keys.nextElement() instanceof String) {
        String key = (String) keys.nextElement();
        String value = newProps.getProperty(key);
        conf.put(key, value);
      }
    }
    return conf;
  }


}

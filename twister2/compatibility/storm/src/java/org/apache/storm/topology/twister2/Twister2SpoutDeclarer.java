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
package org.apache.storm.topology.twister2;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.SpoutDeclarer;

public class Twister2SpoutDeclarer implements SpoutDeclarer {


  private Number maxTaskParallelism;
  private Number maxSpoutPending;
  private Number numTasks;
  private boolean debug;
  private HashMap<String, Object> configuration = new HashMap<>();

  @Override
  public SpoutDeclarer addConfigurations(Map conf) {
    configuration.putAll(conf);
    return this;
  }

  @Override
  public SpoutDeclarer addConfiguration(String config, Object value) {
    this.configuration.put(config, value);
    return this;
  }

  @Override
  public SpoutDeclarer setDebug(boolean debug) {
    this.debug = debug;
    return this;
  }

  @Override
  public SpoutDeclarer setMaxTaskParallelism(Number maxTaskParallelism) {
    this.maxTaskParallelism = maxTaskParallelism;
    return this;
  }

  @Override
  public SpoutDeclarer setMaxSpoutPending(Number maxSpoutPending) {
    this.maxSpoutPending = maxSpoutPending;
    return this;
  }

  @Override
  public SpoutDeclarer setNumTasks(Number numTasks) {
    this.numTasks = numTasks;
    return this;
  }

  //Accessors for twister2

  public Number getMaxTaskParallelism() {
    return maxTaskParallelism;
  }

  public Number getMaxSpoutPending() {
    return maxSpoutPending;
  }

  public Number getNumTasks() {
    return numTasks;
  }

  public boolean isDebug() {
    return debug;
  }

  public HashMap<String, Object> getConfiguration() {
    return configuration;
  }
}

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

import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;

public class Twister2BoltDeclarer implements BoltDeclarer {

  private final HashMap<String, Object> configuration = new HashMap<>();
  private boolean debug;
  private Number maxTaskParallelism;
  private Number maxSpoutPending;
  private Number numTasks;

  @Override
  public BoltDeclarer addConfigurations(Map conf) {
    this.configuration.putAll(conf);
    return this;
  }

  @Override
  public BoltDeclarer addConfiguration(String config, Object value) {
    this.configuration.put(config, value);
    return this;
  }

  @Override
  public BoltDeclarer setDebug(boolean debug) {
    this.debug = debug;
    return this;
  }

  @Override
  public BoltDeclarer setMaxTaskParallelism(Number val) {
    this.maxTaskParallelism = val;
    return this;
  }

  @Override
  public BoltDeclarer setMaxSpoutPending(Number val) {
    this.maxSpoutPending = val;
    return this;
  }

  @Override
  public BoltDeclarer setNumTasks(Number val) {
    this.numTasks = val;
    return this;
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
    return this.fieldsGrouping(componentId, null, fields);
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
    //todo
    return this;
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer allGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer allGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer directGrouping(String componentId) {
    return null;
  }

  @Override
  public BoltDeclarer directGrouping(String componentId, String streamId) {
    return null;
  }

  @Override
  public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
    return null;
  }

  @Override
  public BoltDeclarer customGrouping(String componentId,
                                     String streamId,
                                     CustomStreamGrouping grouping) {
    return null;
  }

  //Accessors for twister2

  public HashMap<String, Object> getConfiguration() {
    return configuration;
  }

  public boolean isDebug() {
    return debug;
  }

  public Number getMaxTaskParallelism() {
    return maxTaskParallelism;
  }

  public Number getMaxSpoutPending() {
    return maxSpoutPending;
  }

  public Number getNumTasks() {
    return numTasks;
  }
}

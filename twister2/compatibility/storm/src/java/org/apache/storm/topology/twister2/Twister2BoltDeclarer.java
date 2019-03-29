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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Twister2BoltDeclarer implements BoltDeclarer, Serializable {

  private final HashMap<String, Object> configuration = new HashMap<>();
  private boolean debugOn;
  private Number maxTParallelism;
  private Number maxSpPending;
  private Number nTasks;

  private transient MadeASourceListener madeASourceListener;

  private List<Twister2BoltGrouping> groupings = new ArrayList<>();

  public Twister2BoltDeclarer(MadeASourceListener madeASourceListener) {
    this.madeASourceListener = madeASourceListener;
  }

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
    this.debugOn = debug;
    return this;
  }

  @Override
  public BoltDeclarer setMaxTaskParallelism(Number val) {
    this.maxTParallelism = val;
    return this;
  }

  @Override
  public BoltDeclarer setMaxSpoutPending(Number val) {
    this.maxSpPending = val;
    return this;
  }

  @Override
  public BoltDeclarer setNumTasks(Number val) {
    this.nTasks = val;
    return this;
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
    return this.fieldsGrouping(componentId, Utils.getDefaultStream(componentId), fields);
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
    this.addGrouping(
        componentId,
        streamId,
        GroupingTechnique.FIELD
    ).setGroupingKey(fields);
    this.madeASourceListener.onMadeASource(componentId);
    return this;
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId) {
    return this.globalGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId, String streamId) {
    //todo
    this.madeASourceListener.onMadeASource(componentId);
    return this;
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId) {
    return this.shuffleGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
    this.addGrouping(componentId, streamId, GroupingTechnique.SHUFFLE);
    this.madeASourceListener.onMadeASource(componentId);
    return this;
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId) {
    return this.localOrShuffleGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
    //todo
    return this;
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId) {
    return this.noneGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId, String streamId) {
    //todo
    return this;
  }

  @Override
  public BoltDeclarer allGrouping(String componentId) {
    return this.allGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer allGrouping(String componentId, String streamId) {
    this.addGrouping(componentId, streamId, GroupingTechnique.ALL);
    return this;
  }

  @Override
  public BoltDeclarer directGrouping(String componentId) {
    return this.directGrouping(componentId, Utils.getDefaultStream(componentId));
  }

  @Override
  public BoltDeclarer directGrouping(String componentId, String streamId) {
    this.addGrouping(componentId, streamId, GroupingTechnique.DIRECT);
    return this;
  }

  @Override
  public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
    return this.customGrouping(componentId, Utils.getDefaultStream(componentId), grouping);
  }

  @Override
  public BoltDeclarer customGrouping(String componentId,
                                     String streamId,
                                     CustomStreamGrouping grouping) {
    //todo
    return this;
  }

  //Utils for twister2
  private Twister2BoltGrouping addGrouping(String componentId,
                                           String streamId,
                                           GroupingTechnique technique) {
    Twister2BoltGrouping t2BoltGrouping = new Twister2BoltGrouping();
    t2BoltGrouping.setComponentId(componentId);
    t2BoltGrouping.setStreamId(streamId);
    t2BoltGrouping.setGroupingTechnique(technique);
    this.groupings.add(t2BoltGrouping);
    return t2BoltGrouping;
  }

  //Accessors for twister2

  public HashMap<String, Object> getConfiguration() {
    return configuration;
  }

  public boolean isDebug() {
    return debugOn;
  }

  public Number getMaxTaskParallelism() {
    return maxTParallelism;
  }

  public Number getMaxSpoutPending() {
    return maxSpPending;
  }

  public Number getNumTasks() {
    return nTasks;
  }

  public List<Twister2BoltGrouping> getGroupings() {
    return groupings;
  }

  //Mutators
}

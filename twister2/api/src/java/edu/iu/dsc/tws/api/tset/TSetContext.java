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
package edu.iu.dsc.tws.api.tset;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;

/**
 * This is the runtime context for a {@link edu.iu.dsc.tws.api.tset.sets.TSet}. This
 * holds the information related to indices, inputs etc. This will be passed on to
 * {@link edu.iu.dsc.tws.api.tset.fn.TFunction} in the runtime, so that corresponding TSet
 * information is available for the functions.
 * <p>
 * This is made {@link Serializable} because, it will be created with instances of
 * {@link edu.iu.dsc.tws.tset.ops.BaseOp} and it would need to be serialized and deserialized to
 * create TaskInstances.
 */
public class TSetContext implements Serializable {
  /**
   * tSet index, which goes from 0 up to the number of parallel tSets
   */
  private int tSetIndex;

  /**
   * Unique id of the tSet
   */
  private String tSetId;

  /**
   * Name of the tSet
   */
  private String tSetName;

  /**
   * Parallel instances of the tSet
   */
  private int parallelism;

  /**
   * tSet specific configurations
   */
  private Map<String, Object> configs;

  /**
   * Inputs that are added to a TSet are stored in this
   * map to be passed to functions
   */
  private Map<String, DataPartition<?>> inputMap;

  /**
   * The worker id this tSet instance belongs to
   */
  private int workerId;

  /**
   * Configuration
   */
  private Config config;

  /**
   * Creates and empty TSet Context
   */
  public TSetContext() {
    this.inputMap = new HashMap<>();
  }

  /**
   * TSet context
   *
   * @param cfg         configuration
   * @param tSetIndex   index
   * @param tSetId      id
   * @param tSetName    name
   * @param parallelism parallelism
   * @param wId         worker id
   * @param configs     configuration
   */
  public TSetContext(Config cfg, int tSetIndex, String tSetId, String tSetName,
                     int parallelism, int wId, Map<String, Object> configs) {
    this.config = cfg;
    this.tSetIndex = tSetIndex;
    this.tSetId = tSetId;
    this.tSetName = tSetName;
    this.parallelism = parallelism;
    this.configs = configs;
    this.workerId = wId;
    this.inputMap = new HashMap<>();
  }

  /**
   * The tSet index
   *
   * @return index
   */
  public int getIndex() {
    return tSetIndex;
  }

  /**
   * tSet id
   *
   * @return the tSet id
   */
  public String getId() {
    return tSetId;
  }

  /**
   * Name of the tSet
   */
  public String getName() {
    return tSetName;
  }

  /**
   * Get the parallelism of the tSet
   *
   * @return number of parallel instances
   */
  public int getParallelism() {
    return parallelism;
  }

  /**
   * Get the worker id this tSet is running
   *
   * @return worker id
   */
  public int getWorkerId() {
    return workerId;
  }

  /**
   * Get the tSet specific configurations
   *
   * @return map of configurations
   */
  public Map<String, Object> getConfigurations() {
    return configs;
  }

  /**
   * Get a configuration with a name
   *
   * @param name name of the config
   * @return the config, if not found return null
   */
  public Object getConfig(String name) {
    return configs.get(name);
  }

  /**
   * Configuration
   *
   * @return configuration
   */
  public Config getConfig() {
    return config;
  }

  /**
   * get the complete input map
   *
   * @return the current input map
   */
  public Map<String, DataPartition<?>> getInputMap() {
    return inputMap;
  }

  /**
   * Retrives the input object that corresponds to the given key
   *
   * @param key key of the input object
   * @return the input object if the key is present or null
   */
  public DataPartition<?> getInput(String key) {
    return inputMap.get(key);
  }

  /**
   * Adds a input object into the map
   *
   * @param key  the key to be associated with the input object
   * @param data the input object
   */
  public void addInput(String key, DataPartition<?> data) {
    inputMap.put(key, data);
  }

  /**
   * Set a new input map
   *
   * @param inputMap the map to be set for this context
   */
  public void setInputMap(Map<String, DataPartition<?>> inputMap) {
    this.inputMap = inputMap;
  }

  /**
   * Adds the given map into {@link TSetContext#inputMap}
   *
   * @param map the map to be added
   */
  public void addInputMap(Map<String, DataPartition<?>> map) {
    this.inputMap.putAll(map);
  }

  /**
   * Sets the ID of the corresponding TSet
   *
   * @param id tset ID
   */
  public void setId(String id) {
    this.tSetId = id;
  }

  /**
   * Sets the tset name
   *
   * @param name tset name
   */
  public void setName(String name) {
    this.tSetName = name;
  }

  /**
   * Sets the TSet parallelism
   *
   * @param parallelism parallelism
   */
  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  /**
   * Updates the runtime information for the TSet instances
   *
   * @param conf    configuration
   * @param taskCtx task context
   */
  public void updateRuntimeInfo(Config conf, TaskContext taskCtx) {
    setConfig(conf);
    settSetIndex(taskCtx.taskIndex());
    setWorkerId(taskCtx.getWorkerId());
  }

  private void setWorkerId(int workerId) {
    this.workerId = workerId;
  }

  private void setConfig(Config config) {
    this.config = config;
  }

  private void settSetIndex(int tSetIndex) {
    this.tSetIndex = tSetIndex;
  }
}

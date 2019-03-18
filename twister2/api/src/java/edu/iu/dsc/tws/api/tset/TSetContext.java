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

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;

public class TSetContext {
  /**
   * tSet index, which goes from 0 up to the number of parallel tSets
   */
  private int tSetIndex;

  /**
   * Unique id of the tSet
   */
  private int tSetId;

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
  private Map<String, Cacheable<?>> inputMap;

  /**
   * The worker id this tSet instance belongs to
   */
  private int workerId;

  /**
   * Configuration
   */
  private Config config;

  /**
   * TSet context
   *
   * @param cfg configuration
   * @param tSetIndex index
   * @param tSetId id
   * @param tSetName name
   * @param parallelism parallelism
   * @param wId worker id
   * @param configs configuration
   */
  public TSetContext(Config cfg, int tSetIndex, int tSetId, String tSetName,
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

  public TSetContext() {
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
  public int getId() {
    return tSetId;
  }

  /**
   * Name of the tSet
   */
  public String getName() {
    return tSetName;
  }

  /**
   * Get the parallism of the tSet
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
  protected Map<String, Cacheable<?>> getInputMap() {
    return inputMap;
  }

  /**
   * Set a new input map
   *
   * @param inputMap the map to be set for this context
   */
  protected void setInputMap(Map<String, Cacheable<?>> inputMap) {
    this.inputMap = inputMap;
  }

  /**
   * Adds the given map into {@link TSetContext#inputMap}
   *
   * @param map the map to be added
   */
  protected void addInputMap(Map<String, Cacheable<?>> map) {
    this.inputMap.putAll(map);
  }

  /**
   * Retrives the input object that corresponds to the given key
   *
   * @param key key of the input object
   * @return the input object if the key is present or null
   */
  public Cacheable<?> getInput(String key) {
    return inputMap.get(key);
  }

  /**
   * Adds a input object into the map
   *
   * @param key the key to be associated with the input object
   * @param data the input object
   */
  public void addInput(String key, Cacheable<?> data) {
    inputMap.put(key, data);
  }

  public int gettSetIndex() {
    return tSetIndex;
  }

  protected void settSetIndex(int tSetIndex) {
    this.tSetIndex = tSetIndex;
  }

  public int gettSetId() {
    return tSetId;
  }

  protected void settSetId(int tSetId) {
    this.tSetId = tSetId;
  }

  public String gettSetName() {
    return tSetName;
  }

  protected void settSetName(String tSetName) {
    this.tSetName = tSetName;
  }

  protected void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  protected void setWorkerId(int workerId) {
    this.workerId = workerId;
  }

  protected void setConfig(Config config) {
    this.config = config;
  }
}

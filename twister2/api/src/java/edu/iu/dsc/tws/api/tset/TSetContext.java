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

import java.util.Map;

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
   * The worker id this tSet instance belongs to
   */
  private int workerId;

  /**
   * TSet context
   *
   * @param tSetIndex
   * @param tSetId
   * @param tSetName
   * @param parallelism
   * @param wId
   * @param configs
   */
  public TSetContext(int tSetIndex, int tSetId, String tSetName,
                     int parallelism, int wId, Map<String, Object> configs) {
    this.tSetIndex = tSetIndex;
    this.tSetId = tSetId;
    this.tSetName = tSetName;
    this.parallelism = parallelism;
    this.configs = configs;
    this.workerId = wId;
  }

  /**
   * The tSet index
   *
   * @return index
   */
  public int tSetIndex() {
    return tSetIndex;
  }

  /**
   * tSet id
   *
   * @return the tSet id
   */
  public int tSetId() {
    return tSetId;
  }

  /**
   * Name of the tSet
   */
  public String tSetName() {
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
}

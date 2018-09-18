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
package edu.iu.dsc.tws.task.graph;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.INode;

/**
 * Users develop ITask object and when it is added to the graph we need additional information
 * including the name, runtime configuration etc.
 */
public class Vertex {

  /**
   * Represents task name
   */
  private String name;

  /**
   * Represents task
   */
  private INode task;

  /**
   * Represents task cpu requirement
   */
  private int cpu;

  /**
   * Represents task memory requirement
   */
  private int memory;

  /**
   * Represents task ram requirement
   */
  private int ram;

  /**
   * Represents task parallelism value
   */
  private int parallelism = 1;

  /**
   * Config value for the task vertex
   */
  private Config config;

  public Vertex(String n, INode t) {
    this.name = n;
    this.task = t;
    config = Config.newBuilder().build();
  }

  public Vertex(String name, INode task, int parallelism) {
    this.name = name;
    this.task = task;
    this.parallelism = parallelism;
    config = Config.newBuilder().build();
  }

  public int getRam() {
    return ram;
  }

  public void setRam(int ram) {
    this.ram = ram;
  }

  public INode getTask() {
    return task;
  }

  public Config getConfig() {
    return config;
  }

  public String getName() {
    return name;
  }

  public int getCpu() {
    return cpu;
  }

  public int getMemory() {
    return memory;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setCpu(int cpu) {
    this.cpu = cpu;
  }

  public void setMemory(int memory) {
    this.memory = memory;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public void addConfiguration(String key, Object val) {
    this.config = Config.newBuilder().put(key, val).putAll(config).build();
  }
}

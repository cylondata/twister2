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
package edu.iu.dsc.tws.api.compute.schedule.elements;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a a worker.
 */
public class Worker {
  /**
   * Id of the worker
   */
  private int id;

  /**
   * CPU assigned to the worker
   */
  private int cpu;

  /**
   * Ram assigned to the worker
   */
  private int ram;

  /**
   * Disk assigned to the worker
   */
  private int disk;

  /**
   * Properties of the worker
   */
  private Map<String, Object> properties;

  /**
   * Create a worker with the id
   *
   * @param id unique id of the worker
   */
  public Worker(int id) {
    this.id = id;
    this.properties = new HashMap<>();
  }

  public void addProperty(String key, Object value) {
    this.properties.put(key, value);
  }

  public Object getProperty(String key) {
    return this.properties.get(key);
  }

  public int getId() {
    return id;
  }

  public int getCpu() {
    return cpu;
  }

  public int getRam() {
    return ram;
  }

  public int getDisk() {
    return disk;
  }

  public void setCpu(int cpu) {
    this.cpu = cpu;
  }

  public void setRam(int ram) {
    this.ram = ram;
  }

  public void setDisk(int disk) {
    this.disk = disk;
  }
}

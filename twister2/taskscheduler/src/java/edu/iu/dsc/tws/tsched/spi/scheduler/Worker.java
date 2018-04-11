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
package edu.iu.dsc.tws.tsched.spi.scheduler;

import java.util.HashMap;
import java.util.Map;

public class Worker {
  private int id;

  private int cpu;
  private int ram;
  private int disk;

  private Map<String, Object> properties;

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

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
package edu.iu.dsc.tws.rsched.spi.resource;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent a resource
 */
public class ResourceContainer {
  private int id;
  // include properties of the resource
  // this can include things like available ports
  private Map<String, Object> properties = new HashMap<>();

  // no of cpus in this container
  private int noOfCpus;

  // memory available to the container
  private int memoryMegaBytes;

  // memory available to the container
  private int diskMegaBytes;

  public ResourceContainer(int id) {
    this.id = id;
  }

  public ResourceContainer(int noOfCpus, int memoryMegaBytes) {
    this.noOfCpus = noOfCpus;
    this.memoryMegaBytes = memoryMegaBytes;
  }

  public ResourceContainer(int noOfCpus, int memoryMegaBytes, int diskMegaBytes) {
    this.noOfCpus = noOfCpus;
    this.memoryMegaBytes = memoryMegaBytes;
    this.diskMegaBytes = diskMegaBytes;
  }

  public int getId() {
    return id;
  }

  public void addProperty(String key, Object property) {
    properties.put(key, property);
  }

  public Object getProperty(String key) {
    return properties.get(key);
  }

  public int getNoOfCpus() {
    return noOfCpus;
  }

  public int getMemoryMegaBytes() {
    return memoryMegaBytes;
  }

  public long getMemoryInBytes() {
    return memoryMegaBytes * 1024 * 1024L;
  }

  public int getDiskMegaBytes() {
    return diskMegaBytes;
  }

  public long getDiskInBytes() {
    return diskMegaBytes * 1024 * 1024L;
  }

}

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
package edu.iu.dsc.tws.tsched.spi.taskschedule;

/**
 * This class is internal to the task scheduler to get and set the required ram, disk, and cpu
 * values to the task instances.
 */
public class Resource {

  private Double ram;
  private Double disk;
  private Double cpu;

  public int getId() {
    return id;
  }

  public Resource setId(int idx) {
    this.id = idx;
    return this;
  }

  private int id;

  public Resource(Double ram, Double disk, Double cpu) {
    this.ram = ram;
    this.disk = disk;
    this.cpu = cpu;
  }

  public Resource(Double ram, Double disk, Double cpu, Integer idx) {
    this.ram = ram;
    this.disk = disk;
    this.cpu = cpu;
    this.id = idx;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Resource) {
      Resource r = (Resource) o;
      return (this.getCpu() == r.getCpu())
          && (this.getRam().equals(r.getRam()))
          && (this.getDisk().equals(r.getDisk()));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(cpu);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + ram.hashCode();
    result = 31 * result + disk.hashCode();
    return result;
  }

  public Double getRam() {
    return ram;
  }

  public void setRam(Double ram) {
    this.ram = ram;
  }

  public Double getDisk() {
    return disk;
  }

  public void setDisk(Double disk) {
    this.disk = disk;
  }

  public Double getCpu() {
    return cpu;
  }

  public void setCpu(Double cpu) {
    this.cpu = cpu;
  }

  public Resource cloneWithRam(double newRam) {
    return new Resource(newRam, this.getDisk(), this.getCpu());
  }

  //This will be used in future.
  public Resource cloneWithRam(double newRam, double newDisk, double newCpu) {
    return new Resource(newRam, newDisk, newCpu);
  }
}



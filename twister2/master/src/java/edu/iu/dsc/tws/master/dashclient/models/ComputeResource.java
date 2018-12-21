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
package edu.iu.dsc.tws.master.dashclient.models;

import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * The ComputeResource to send with json to Dashboard from JobMaster
 */

public class ComputeResource {
  private double cpu;
  private int ram;
  private double disk;
  private int index;
  private int instances;
  private boolean scalable;

  public ComputeResource() { }

  public ComputeResource(JobAPI.ComputeResource computeResource) {
    cpu = computeResource.getCpu();
    ram = computeResource.getRamMegaBytes();
    disk = computeResource.getDiskGigaBytes();
    index = computeResource.getIndex();
    instances = computeResource.getInstances();
    scalable = computeResource.getScalable();
  }

  public double getCpu() {
    return cpu;
  }

  public int getRam() {
    return ram;
  }

  public double getDisk() {
    return disk;
  }

  public int getIndex() {
    return index;
  }

  public int getInstances() {
    return instances;
  }

  public boolean getScalable() {
    return scalable;
  }

  public void setCpu(double cpu) {
    this.cpu = cpu;
  }

  public void setRam(int ram) {
    this.ram = ram;
  }

  public void setDisk(double disk) {
    this.disk = disk;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public void setInstances(int instances) {
    this.instances = instances;
  }

  public void setScalable(boolean scalable) {
    this.scalable = scalable;
  }
}

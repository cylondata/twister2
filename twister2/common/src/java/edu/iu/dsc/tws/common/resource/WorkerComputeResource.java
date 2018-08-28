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
package edu.iu.dsc.tws.common.resource;

import java.util.Objects;

/**
 * Represent a resource for a worker
 * It is used to define a resource when submitting the job
 * It is also used when providing the worker resources to IWorker from resource scheduler
 * In the second case, id is also used.
 * When defining worker resources for a job submission, id is not used
 */
public class WorkerComputeResource {
  private int id;

  // no of cpus in this container
  // it can be a fractional number such as 0.5
  private double noOfCpus;

  // memory available to the container
  private int memoryMegaBytes;

  // volatile disk space available to the container
  private double diskGigaBytes;

  public WorkerComputeResource(int id) {
    this.id = id;
  }

  public WorkerComputeResource(double noOfCpus, int memoryMegaBytes) {
    this.noOfCpus = noOfCpus;
    this.memoryMegaBytes = memoryMegaBytes;
  }

  public WorkerComputeResource(double noOfCpus, int memoryMegaBytes, double diskGigaBytes) {
    this.noOfCpus = noOfCpus;
    this.memoryMegaBytes = memoryMegaBytes;
    this.diskGigaBytes = diskGigaBytes;
  }

  public WorkerComputeResource(int id, double noOfCpus, int memoryMegaBytes, double diskGigaBytes) {
    this.id = id;
    this.noOfCpus = noOfCpus;
    this.memoryMegaBytes = memoryMegaBytes;
    this.diskGigaBytes = diskGigaBytes;
  }

  public int getId() {
    return id;
  }

  public double getNoOfCpus() {
    return noOfCpus;
  }

  public int getMemoryMegaBytes() {
    return memoryMegaBytes;
  }

  public long getMemoryInBytes() {
    return memoryMegaBytes * 1024 * 1024L;
  }

  public double getDiskGigaBytes() {
    return diskGigaBytes;
  }

  public int getDiskMegaBytes() {
    return (int) (diskGigaBytes * 1024);
  }

  public long getDiskInBytes() {
    return (long) (diskGigaBytes * 1024 * 1024 * 1024);
  }

  /**
   * only id based equality
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkerComputeResource that = (WorkerComputeResource) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {

    return Objects.hash(id);
  }

}

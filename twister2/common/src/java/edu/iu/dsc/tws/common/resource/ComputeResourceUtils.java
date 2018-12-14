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

import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class ComputeResourceUtils {

  private ComputeResourceUtils() { }

  public static JobAPI.ComputeResource createComputeResource(int index,
                                                             double cpu,
                                                             int ramMegaBytes,
                                                             double diskGigaBytes) {
    return JobAPI.ComputeResource.newBuilder()
        .setIndex(index)
        .setCpu(cpu)
        .setRamMegaBytes(ramMegaBytes)
        .setDiskGigaBytes(diskGigaBytes)
        .build();
  }

  public static JobAPI.ComputeResource updateComputeResourceIndex(int newIndex,
                                                           JobAPI.ComputeResource computeResource) {
    return JobAPI.ComputeResource.newBuilder()
        .setIndex(newIndex)
        .setCpu(computeResource.getCpu())
        .setRamMegaBytes(computeResource.getRamMegaBytes())
        .setDiskGigaBytes(computeResource.getDiskGigaBytes())
        .setInstances(computeResource.getInstances())
        .setScalable(computeResource.getScalable())
        .setWorkersPerPod(computeResource.getWorkersPerPod())
        .build();
  }

  public static long getRamInBytes(JobAPI.ComputeResource computeResource) {
    return computeResource.getRamMegaBytes() * 1024L * 1024;
  }

  public static long getDiskInBytes(JobAPI.ComputeResource computeResource) {
    return (long) (computeResource.getDiskGigaBytes() * 1024 * 1024 * 1024);
  }

  /**
   * covert the given ComputeResource to a one line String for logging output
   * @return
   */
  public static String toString(JobAPI.ComputeResource cr) {
    return String.format("ComputeResource[%d]: cpu: %.1f, ram: %d MB, disk: %.1f GB, "
            + "instances: %d, scalable: %b workersPerPod: %d",
        cr.getIndex(), cr.getCpu(), cr.getRamMegaBytes(), cr.getDiskGigaBytes(),
        cr.getInstances(), cr.getScalable(), cr.getWorkersPerPod());
  }

}

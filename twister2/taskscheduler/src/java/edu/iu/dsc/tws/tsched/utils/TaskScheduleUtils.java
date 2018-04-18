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
package edu.iu.dsc.tws.tsched.utils;

import java.util.Map;

import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;

public final class TaskScheduleUtils {

  private static final Double MIN_RAM_PER_INSTANCE = 200.0;

  private TaskScheduleUtils() {
  }

  public static Resource getResourceRequirement(String taskName,
                                                Map<String, Double> componentRamMap,
                                                Resource defaultInstanceResource,
                                                Resource maxContainerResource,
                                                int paddingPercentage) {

    double instanceRam = defaultInstanceResource.getRam();
    double instanceDisk = defaultInstanceResource.getDisk();
    double instanceCpu = defaultInstanceResource.getCpu();

    if (componentRamMap.containsKey(taskName)) {
      instanceRam = componentRamMap.get(taskName);
      instanceDisk = componentRamMap.get(taskName);
      instanceCpu = componentRamMap.get(taskName);
    }
    /*assertIsValidInstance(defaultInstanceResource.cloneWithRam(
        instanceRam, instanceDisk, instanceCpu),
        MIN_RAM_PER_INSTANCE, maxContainerResource, paddingPercentage);
    return defaultInstanceResource.cloneWithRam(instanceRam, instanceDisk, instanceCpu);*/

    assertIsValidInstance(defaultInstanceResource.cloneWithRam(instanceRam),
        MIN_RAM_PER_INSTANCE, maxContainerResource, paddingPercentage);
    return defaultInstanceResource.cloneWithRam(instanceRam);
  }

  private static void assertIsValidInstance(Resource instanceResources,
                                            double minInstanceRam,
                                            Resource maxContainerResources,
                                            int paddingPercentage) throws TaskSchedulerException {

    if (instanceResources.getRam() < minInstanceRam) {
      throw new TaskSchedulerException(String.format(
          "Instance requires ram %s which is less than the minimum ram per instance of %s",
          instanceResources.getRam(), minInstanceRam));
    }

    double instanceRam = Math.round(TaskScheduleUtils.increaseBy(
        instanceResources.getRam(), paddingPercentage));
    if (instanceRam > maxContainerResources.getRam()) {
      throw new TaskSchedulerException(String.format(
          "This instance requires containers of at least %s ram. The current max container "
              + "size is %s",
          instanceRam, maxContainerResources.getRam()));
    }

    double instanceDisk = Math.round(TaskScheduleUtils.increaseBy(
        instanceResources.getDisk(), paddingPercentage));
    if (instanceDisk > maxContainerResources.getDisk()) {
      throw new TaskSchedulerException(String.format(
          "This instance requires containers of at least %s disk. The current max container"
              + "size is %s",
          instanceDisk, maxContainerResources.getDisk()));
    }

    double instanceCpu = Math.round(TaskScheduleUtils.increaseBy(
        instanceResources.getCpu(), paddingPercentage));
    if (instanceCpu > maxContainerResources.getCpu()) {
      throw new TaskSchedulerException(String.format(
          "This instance requires containers with at least %s cpu cores. The current max container"
              + "size is %s cores",
          instanceCpu > maxContainerResources.getCpu(), maxContainerResources.getCpu()));
    }
  }

  public static long increaseBy(long value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }

  public static double increaseBy(double value, int paddingPercentage) {
    return value + (paddingPercentage * value) / 100;
  }
}

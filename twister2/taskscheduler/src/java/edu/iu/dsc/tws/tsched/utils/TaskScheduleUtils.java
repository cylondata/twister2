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

import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;

/**
 * This is the util class for the task scheduler to get the resource value of task instance and
 * validate the minimum value of the task instance ram value. And,
 */
public final class TaskScheduleUtils {

  private static final Double MIN_RAM_PER_INSTANCE =
      TaskSchedulerContext.TWISTER2_TASK_INSTANCE_RAM_DEFAULT;

  private TaskScheduleUtils() {
  }

  /**
   * This method gets the resource requirement of task instances, validate and clone the required
   * task instance ram value.
   * @param taskName
   * @param taskRamMap
   * @param defaultInstanceResource
   * @param maxContainerResource
   * @param paddingPercentage
   * @return
   */
  public static Resource getResourceRequirement(String taskName,
                                                Map<String, Double> taskRamMap,
                                                Resource defaultInstanceResource,
                                                Resource maxContainerResource,
                                                int paddingPercentage) {

    double instanceRam = defaultInstanceResource.getRam();
    double instanceDisk = defaultInstanceResource.getDisk();
    double instanceCpu = defaultInstanceResource.getCpu();

    if (taskRamMap.containsKey(taskName)) {
      instanceRam = taskRamMap.get(taskName);
      instanceDisk = taskRamMap.get(taskName);
      instanceCpu = taskRamMap.get(taskName);
    }

    //In future, it will be validated for the task instance disk and cpu values.

    /*assertIsValidInstance(defaultInstanceResource.cloneWithRam(
        instanceRam, instanceDisk, instanceCpu),
        MIN_RAM_PER_INSTANCE, maxContainerResource, paddingPercentage);
    return defaultInstanceResource.cloneWithRam(instanceRam, instanceDisk, instanceCpu);*/

    assertIsValidInstance(defaultInstanceResource.cloneWithRam(instanceRam),
        maxContainerResource, paddingPercentage);
    return defaultInstanceResource.cloneWithRam(instanceRam);
  }

  /**
   * This method is to make sure that each task instance should satisfy the minimum ram value. Also,
   * after increasing the padding percentage of task instance ram, disk, and cpu value it shouldn't
   * go beyond the maximum container resource values (ram, disk, and cpu).
   *
   * @param instanceResources
   * @param maxContainerResources
   * @param paddingPercentage
   * @throws TaskSchedulerException
   */
  private static void assertIsValidInstance(Resource instanceResources,
                                            Resource maxContainerResources,
                                            int paddingPercentage) throws TaskSchedulerException {

    if (instanceResources.getRam() < (double) TaskScheduleUtils.MIN_RAM_PER_INSTANCE) {
      throw new TaskSchedulerException(String.format(
          "Instance requires ram %s which is less than the minimum ram per instance of %s",
          instanceResources.getRam(), TaskScheduleUtils.MIN_RAM_PER_INSTANCE));
    }

    /*To increase the task instance ram value which is up to the padding percentage specified in the
    configuration file. After padding the task instance ram value, if it reaches beyond the
    maximum container value, then it will throw the exception.*/

    double instanceRam = Math.round(TaskScheduleUtils.increaseBy(
        instanceResources.getRam(), paddingPercentage));
    if (instanceRam > maxContainerResources.getRam()) {
      throw new TaskSchedulerException(String.format(
          "This instance requires containers of at least %s ram. The current max container "
              + "size is %s",
          instanceRam, maxContainerResources.getRam()));
    }

    /*To increase the task instance disk value which is up to the padding percentage specified in
     the configuration file. After padding the task instance ram value, if it reaches beyond the
     maximum container value, then it will throw the exception.*/

    double instanceDisk = Math.round(TaskScheduleUtils.increaseBy(
        instanceResources.getDisk(), paddingPercentage));
    if (instanceDisk > maxContainerResources.getDisk()) {
      throw new TaskSchedulerException(String.format(
          "This instance requires containers of at least %s disk. The current max container"
              + "size is %s",
          instanceDisk, maxContainerResources.getDisk()));
    }

    /*To increase the task instance cpu value which is up to the padding percentage specified in the
    configuration file. After padding the task instance cpu value, if it reaches beyond the
    maximum container value, then it will throw the exception.*/

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

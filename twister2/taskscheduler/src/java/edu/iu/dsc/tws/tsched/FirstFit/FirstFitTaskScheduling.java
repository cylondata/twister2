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

package edu.iu.dsc.tws.tsched.FirstFit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;

import edu.iu.dsc.tws.tsched.builder.TaskSchedulePlanBuilder;

import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.common.Context;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;
import edu.iu.dsc.tws.tsched.utils.RequiredRam;
import edu.iu.dsc.tws.tsched.utils.RequiredDisk;
import edu.iu.dsc.tws.tsched.utils.RequiredCPU;
import edu.iu.dsc.tws.tsched.builder.Container;
import edu.iu.dsc.tws.tsched.builder.ContainerIdScorer;


public class FirstFitTaskScheduling {

    //These values should be replaced with an appropriate values...
    private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
    private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
    private static final double MIN_RAM_PER_INSTANCE = 180;
    private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
    private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;

    private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
    private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;
    /////////////////////////////////////////////////////////////////

    private Job job;
    private Config config;
    private Resource defaultResourceValue;
    private Resource maximumContainerResourceValue;
    private int paddingPercentage;

    private Double instanceRAM;
    private Double instanceDisk;
    private Double instanceCPU;

    private int numContainers;
    private Double containerRAMValue;
    private Double containerDiskValue;
    private Double containerCPUValue;


    public void initialize(Config config, Job job) {
        //this.config = config;
       this.job = job;
       this.defaultResourceValue = new Resource(Context.instanceRam (config),
                                   Context.instanceDisk (config), Context.instanceCPU (config));
       this.paddingPercentage = JobAttributes.JOB_CONTAINER_PADDING_PERCENTAGE;
       instanceRAM = this.defaultResourceValue.getRam ();
       instanceDisk = this.defaultResourceValue.getDisk ();
       instanceCPU = this.defaultResourceValue.getCpu ();

       //This value will be calculated by adding the container percentage value....
       this.maximumContainerResourceValue = new Resource(JobAttributes.JOB_CONTAINER_MAX_RAM_VALUE,
                                            JobAttributes.JOB_CONTAINER_MAX_DISK_VALUE,
                                            JobAttributes.JOB_CONTAINER_MAX_CPU_VALUE);
       System.out.println("instance default values:"+instanceRAM+"\t"+instanceDisk+"\t"+instanceCPU);
    }

    private TaskSchedulePlanBuilder newTaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskSchedulePlan) {

        return new TaskSchedulePlanBuilder(job.getJobId (), previousTaskSchedulePlan)
                .setContainerMaximumResourceValue(maximumContainerResourceValue)
                .setInstanceDefaultResourceValue(defaultResourceValue)
                .setRequestedContainerPadding(paddingPercentage)
                .setTaskRamMap (JobAttributes.getTaskRamMap (job))
                .setTaskDiskMap (JobAttributes.getTaskDiskMap (job))
                .setTaskCPUMap (JobAttributes.getTaskCPUMap (job));
    }


    public TaskSchedulePlan tschedule() throws ScheduleException {

        TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder (null);
        System.out.println("Task schedule plan container maximum ram value is:\t"+taskSchedulePlanBuilder.getContainerMaximumResourceValue ().getDisk ());
        taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
        return taskSchedulePlanBuilder.build();

    }

    public TaskSchedulePlanBuilder FirstFitFTaskSchedulingAlgorithm(TaskSchedulePlanBuilder taskSchedulePlanBuilder) {

        Map<String, Integer> parallelTaskMap = JobAttributes.getParallelTaskMap (job);
        assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
        System.out.println("Parallel TaskMap:"+parallelTaskMap.keySet ());
        return taskSchedulePlanBuilder;

    }

    public void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder, Map<String, Integer> parallelTaskMap) {

        ArrayList<RequiredRam> ramRequirements = getSortedRAMInstances(parallelTaskMap.keySet());
        for (RequiredRam ramRequirement : ramRequirements) {
            String taskName = ramRequirement.getTaskName();
            int numberOfInstances = parallelTaskMap.get(taskName);
            System.out.println("Number of Instances for the task name:"+numberOfInstances+"\t"+taskName);
            for (int j = 0; j < numberOfInstances; j++) {
                FirstFitInstanceAllocation(taskSchedulePlanBuilder, taskName);
                System.out.println("I am inside assign instances to container function:"+taskSchedulePlanBuilder.getTaskDiskMap ()
                        +"\t"+taskName);
            }
        }
    }

    public void FirstFitInstanceAllocation(TaskSchedulePlanBuilder taskSchedulePlanBuilder, String taskName) {

        if (this.numContainers == 0) {
           taskSchedulePlanBuilder.updateNumContainers(++numContainers);
        }
        try {
            //taskSchedulePlanBuilder.addInstance(taskName);
            taskSchedulePlanBuilder.addInstance (new ContainerIdScorer (), taskName);
        }
        catch (Exception e) {
            e.printStackTrace ();
            taskSchedulePlanBuilder.updateNumContainers(++numContainers);
            taskSchedulePlanBuilder.addInstance(numContainers, taskName);
        }
    }

    private ArrayList<RequiredRam> getSortedRAMInstances(Set<String> taskNameSet) {

        Job job = new Job();
        job.setJob (job);

        ArrayList<RequiredRam> ramRequirements = new ArrayList<>();
        Map<String, Double> taskRamMap = JobAttributes.getTaskRamMap (job);
        for (String taskName : taskNameSet) {
            /*Resource requiredResource = PackingUtils.getResourceRequirement(
                    taskName, ramMap, this.defaultResourceValue,
                    this.maximumContainerResourceValue, this.paddingPercentage);*/  //It should be modified in future....
            //ramRequirements.add(new RequiredRam(taskName, requiredResource.getRam()));

           if(taskRamMap.containsKey (taskNameSet)){
                instanceRAM = taskRamMap.get(taskNameSet);
           }
           //RequiredRam requiredRam = new RequiredRam (taskName, instanceRAM);
           //ramRequirements.add (requiredRam);
           ramRequirements.add(new RequiredRam (taskName, instanceRAM));
           System.out.println("Task Name and Required Ram:"+taskName+"\t"+instanceRAM);
        }
        Collections.sort(ramRequirements, Collections.reverseOrder());
        return ramRequirements;
    }

   /*private static int getLargestContainerSize(Map<Integer, List<InstanceId>> InstancesAllocation) {
        int max = 0;
        for (List<InstanceId> instances : InstancesAllocation.values ()) {
            if (instances.size () > max) {
                max = instances.size ();
            }
        }
        System.out.println("Maximum container value is:\t"+max);
        return max;
    }*/

    private static double getContainerCpuValue(Map<Integer, List<TaskInstanceId>> InstancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig= job.getJobConfig().getKvsList();
        double defaultContainerCpu =
                DEFAULT_CPU_PADDING_PER_CONTAINER + getLargestContainerSize(InstancesAllocation);

        String cpuHint = JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
                Double.toString(defaultContainerCpu)); */

        //These two lines will be removed with the above commented code, once the actual job description file is created...
        String cpuHint = "0.6";
        return Double.parseDouble (cpuHint);
    }

    private static Double getContainerDiskValue(Map<Integer, List<TaskInstanceId>> InstancesAllocation) {

        /*ByteAmount defaultContainerDisk = instanceDiskDefault
                .multiply(getLargestContainerSize(InstancesAllocation))
                .plus(DEFAULT_DISK_PADDING_PER_CONTAINER);

        List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(jobConfig,
                com.tws.api.Config.JOB_CONTAINER_DISK_REQUESTED,
                defaultContainerDisk); */

        //These two lines will be removed with the above commented code, once the actual job description file is created...
        Long containerDiskValue = 100L;
        return containerDiskValue.doubleValue ();
    }

    private static Double getContainerRamValue(Map<Integer, List<TaskInstanceId>> InstancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.JOB_CONTAINER_RAM_REQUESTED,
                NOT_SPECIFIED_NUMBER_VALUE);*/

        //These two lines will be removed with the above commented code, once the actual job description file is created...
        //return ByteAmount.fromGigabytes (containerRAMValue);
        Long containerRAMValue = 10L;
        return containerRAMValue.doubleValue ();
    }

    //This method will be implemented for future rescheduling case....
    public void reschedule (){

    }

    public void close() {

    }
}

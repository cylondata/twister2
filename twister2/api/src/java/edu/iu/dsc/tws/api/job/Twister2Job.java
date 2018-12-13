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
package edu.iu.dsc.tws.api.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.resource.ComputeResourceUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

/**
 * This is a basic job with only communication available
 */
public final class Twister2Job {

  private static final Logger LOG = Logger.getLogger(Twister2Job.class.getName());

  private static final KryoSerializer KRYO_SERIALIZER = new KryoSerializer();

  private String jobName;
  private String workerClass;
  private ArrayList<JobAPI.ComputeResource> computeResources = new ArrayList<>();
  private JobConfig config;

  private Twister2Job() {
  }

  /**
   * Serializing the JobAPI
   **/
  public JobAPI.Job serialize() {

    checkJobParameters();
    updateIndexesForComputeResources();

    JobAPI.Job.Builder jobBuilder = JobAPI.Job.newBuilder();

    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();

    config.forEach((key, value) -> {
      byte[] objectByte = KRYO_SERIALIZER.serialize(value);
      configBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
    });

    jobBuilder.setConfig(configBuilder);
    jobBuilder.setWorkerClassName(workerClass);
    jobBuilder.setJobName(jobName);
    jobBuilder.setNumberOfWorkers(countNumberOfWorkers());

    for (JobAPI.ComputeResource computeResource: computeResources) {
      jobBuilder.addComputeResource(computeResource);
    }

    return jobBuilder.build();
  }

  /**
   * make sure there is at most one scalable ComputeResource
   * put the scalable ComputeResource to the end of the list
   * the scalable ComputeResource will have the highest index number in the list
   */
  private void updateIndexesForComputeResources() {

    int scalableCount = countScalableComputeResources();
    if (scalableCount > 1) {
      throw new RuntimeException("There can be at most one scalabe ComputeResource in a job. "
          + scalableCount + " scalabe ComputeResource requested. Aborting submission. +++++++");

      // if there is no scalable ComputeResource, nothing to be done.
      // use the original indexes assigned
    } else if (scalableCount == 0) {
      return;

      // if we are at this point, it means that there is only one scalable ComputeResource
      // check whether the last one is scalable, if so, nothing to be done
    } else if (computeResources.get(computeResources.size() - 1).getScalable()) {
      return;
    }

    // put the scalable ComputeResource to the end of the list
    int scalableIndex = indexOfFirstScalableComputeResource();
    JobAPI.ComputeResource scalableComputeResource = computeResources.remove(scalableIndex);
    computeResources.add(scalableComputeResource);

    // update indexes for ComputeResource that comes after scalable ComputeResource
    for (int i = scalableIndex; i < computeResources.size(); i++) {
      JobAPI.ComputeResource updatedCR =
          ComputeResourceUtils.updateComputeResourceIndex(i, computeResources.get(i));

      computeResources.set(i, updatedCR);
    }

  }

  /**
   * find the index of the first scalable ComputeResource in the list
   * @return
   */
  private int indexOfFirstScalableComputeResource() {

    int index = 0;
    for (JobAPI.ComputeResource computeResource: computeResources) {
      if (computeResource.getScalable()) {
        return index;
      }

      index++;
    }

    return -1;
  }

  /**
   * count the number of scalable indexes
   * @return
   */
  private int countScalableComputeResources() {

    int counter = 0;
    for (JobAPI.ComputeResource computeResource: computeResources) {
      if (computeResource.getScalable()) {
        counter++;
      }
    }

    return counter;
  }

  private void checkJobParameters() {
    if (jobName == null) {
      throw new RuntimeException("Job jobName is null. You have to provide a unique jobName");
    }

    if (workerClass == null) {
      throw new RuntimeException("workerClass is null. A worker class has to be provided.");
    }

    if (computeResources.size() == 0) {
      throw new RuntimeException("No ComputeResource is provided.");
    }

    if (countNumberOfWorkers() == 0) {
      throw new RuntimeException("0 worker instances requested.");
    }
  }

  /**
   * we only allow job jobName to be updated through this interface
   * @param jobName
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return jobName;
  }

  public String getWorkerClass() {
    return workerClass;
  }

  public ArrayList<JobAPI.ComputeResource> getComputeResources() {
    return computeResources;
  }

  public int getNumberOfWorkers() {
    return countNumberOfWorkers();
  }

  public JobConfig getConfig() {
    return config;
  }

  private int countNumberOfWorkers() {
    int totalWorkers = 0;
    for (JobAPI.ComputeResource computeResource: computeResources) {
      totalWorkers += computeResource.getInstances() * computeResource.getWorkersPerPod();
    }
    return totalWorkers;
  }

  public static Twister2Job loadTwister2Job(Config config, JobConfig jobConfig) {
    // build and return the job
    return Twister2Job.newBuilder()
        .setJobName(Context.jobName(config))
        .setWorkerClass(SchedulerContext.workerClass(config))
        .loadComputeResources(config)
        .setConfig(jobConfig)
        .build();

  }

  public static Twister2JobBuilder newBuilder() {
    return new Twister2JobBuilder();
  }

  public static final class Twister2JobBuilder {
    private Twister2Job twister2Job;
    private int computeIndexCounter = 0;

    private Twister2JobBuilder() {
      this.twister2Job = new Twister2Job();
    }

    public Twister2JobBuilder setJobName(String name) {
      twister2Job.jobName = name;
      return this;
    }

    public Twister2JobBuilder setWorkerClass(String workerClass) {
      twister2Job.workerClass = workerClass;
      return this;
    }

    public Twister2JobBuilder setWorkerClass(Class<? extends IWorker> workerClass) {
      twister2Job.workerClass = workerClass.getName();
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 int instances) {
      addComputeResource(cpu, ramMegaBytes, 0, instances, false, 1);
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 int instances,
                                                 boolean scalable) {
      addComputeResource(cpu, ramMegaBytes, 0, instances, scalable, 1);
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 double diskGigaBytes,
                                                 int instances) {
      addComputeResource(cpu, ramMegaBytes, diskGigaBytes, instances, false, 1);
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 double diskGigaBytes,
                                                 int instances,
                                                 boolean scalabe) {
      addComputeResource(cpu, ramMegaBytes, diskGigaBytes, instances, scalabe, 1);
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 double diskGigaBytes,
                                                 int instances,
                                                 int workersPerPod) {

      addComputeResource(cpu, ramMegaBytes, diskGigaBytes, instances, false, workersPerPod);
      return this;
    }

    public Twister2JobBuilder addComputeResource(double cpu,
                                                 int ramMegaBytes,
                                                 double diskGigaBytes,
                                                 int instances,
                                                 boolean scalable,
                                                 int workersPerPod) {
      JobAPI.ComputeResource computeResource = JobAPI.ComputeResource.newBuilder()
          .setCpu(cpu)
          .setRamMegaBytes(ramMegaBytes)
          .setDiskGigaBytes(diskGigaBytes)
          .setInstances(instances)
          .setScalable(scalable)
          .setWorkersPerPod(workersPerPod)
          .setIndex(computeIndexCounter++)
          .build();

      twister2Job.computeResources.add(computeResource);
      return this;
    }

    @SuppressWarnings({"unchecked", "COMPACT_NO_ARRAY"})
    private Twister2JobBuilder loadComputeResources(Config config) {
      // to remove unchecked cast warnings:
      // https://stackoverflow.com/questions/19163453/
      // how-to-check-types-of-key-and-value-if-object-instanceof-hashmap
      List<Map<String, Object>> list =
          (List) (config.get(SchedulerContext.WORKER_COMPUTE_RESOURCES));

      for (Map<String, Object> computeResource: list) {
        double cpu = (Double) computeResource.get("cpu");
        int ram = (Integer) computeResource.get("ram");
        double disk = (Double) computeResource.get("disk");
        int instances = (Integer) computeResource.get("instances");
        boolean scalable = false;
        if (computeResource.get("scalable") != null) {
          scalable = (Boolean) computeResource.get("scalable");
        }

        int workersPerPod = 1;
        if (computeResource.get("workersPerPod") != null) {
          workersPerPod = (Integer) computeResource.get("workersPerPod");
        }

        addComputeResource(cpu, ram, disk, instances, scalable, workersPerPod);
      }

      return this;
    }

    public Twister2JobBuilder setConfig(JobConfig config) {
      twister2Job.config = config;
      return this;
    }

    public Twister2Job build() {
      if (twister2Job.config == null) {
        twister2Job.config = new JobConfig();
      }
      return twister2Job;
    }

  }
}

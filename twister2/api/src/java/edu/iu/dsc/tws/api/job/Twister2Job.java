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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This is a basic job with only communication available
 */
public final class Twister2Job {

  private static final Logger LOG = Logger.getLogger(Twister2Job.class.getName());

  private static final KryoSerializer KRYO_SERIALIZER = new KryoSerializer();

  private String name;
  private String workerClass;
  private HashMap<JobAPI.ComputeResource, Integer> resources = new HashMap<>();
  private JobConfig config;

  private Twister2Job() {
  }

  /**
   * Serializing the JobAPI
   **/
  public JobAPI.Job serialize() {
    JobAPI.Job.Builder jobBuilder = JobAPI.Job.newBuilder();

    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();

    config.forEach((key, value) -> {
      byte[] objectByte = KRYO_SERIALIZER.serialize(value);
      configBuilder.putConfigByteMap(key, ByteString.copyFrom(objectByte));
    });

    jobBuilder.setConfig(configBuilder);
    jobBuilder.setWorkerClassName(workerClass);
    jobBuilder.setJobName(name);
    jobBuilder.setNumberOfWorkers(countNumberOfWorkers());

    JobAPI.JobResources.Builder jobResourcesBuilder = JobAPI.JobResources.newBuilder();

    for (Map.Entry<JobAPI.ComputeResource, Integer> entry: resources.entrySet()) {

      JobAPI.JobResources.ResourceSet.Builder resourceSetBuilder =
          JobAPI.JobResources.ResourceSet.newBuilder();
      resourceSetBuilder.setNumberOfWorkers(entry.getValue());
      resourceSetBuilder.setComputeResource(
          JobAPI.ComputeResource.newBuilder()
              .setCpu(entry.getKey().getCpu())
              .setRamMegaBytes(entry.getKey().getRamMegaBytes())
              .setDiskGigaBytes(entry.getKey().getDiskGigaBytes())
              .build()
      );
      jobResourcesBuilder.addResource(resourceSetBuilder.build());
    }

    // set the job resources
    jobBuilder.setJobResources(jobResourcesBuilder.build());

    return jobBuilder.build();
  }

  /**
   * we only allow job name to be updated through this interface
   * @param jobName
   */
  public void setName(String jobName) {
    name = jobName;
  }

  public String getName() {
    return name;
  }

  public String getWorkerClass() {
    return workerClass;
  }

  public HashMap<JobAPI.ComputeResource, Integer> getComputeResourceMap() {
    return resources;
  }

  public int getNumberOfWorkers() {
    return countNumberOfWorkers();
  }

  public JobConfig getConfig() {
    return config;
  }

  private int countNumberOfWorkers() {
    int totalWorkers = 0;
    for (Integer workersPerSet: resources.values()) {
      totalWorkers += workersPerSet;
    }
    return totalWorkers;
  }

  public static BasicJobBuilder newBuilder() {
    return new BasicJobBuilder();
  }

  public static final class BasicJobBuilder {
    private Twister2Job twister2Job;

    private BasicJobBuilder() {
      this.twister2Job = new Twister2Job();
    }

    public BasicJobBuilder setName(String name) {
      twister2Job.name = name;
      return this;
    }

    public BasicJobBuilder setWorkerClass(String workerClass) {
      twister2Job.workerClass = workerClass;
      return this;
    }

    public BasicJobBuilder setWorkerClass(Class<? extends IWorker> workerClass) {
      twister2Job.workerClass = workerClass.getName();
      return this;
    }

    public BasicJobBuilder addComputeResource(double cpu, int ramMegaBytes, int numberOfWorkers) {
      addComputeResource(cpu, ramMegaBytes, 0, numberOfWorkers);
      return this;
    }


    public BasicJobBuilder addComputeResource(double cpu, int ramMegaBytes, double diskGigABytes,
                                              int numberOfWorkers) {
      JobAPI.ComputeResource computeResource = JobAPI.ComputeResource.newBuilder()
          .setCpu(cpu)
          .setRamMegaBytes(ramMegaBytes)
          .setDiskGigaBytes(diskGigABytes)
          .build();
      addComputeResource(computeResource, numberOfWorkers);
      return this;
    }

    public BasicJobBuilder addComputeResource(JobAPI.ComputeResource computeResource,
                                          int numberOfWorkers) {
      twister2Job.resources.put(computeResource, numberOfWorkers);
      return this;
    }

    public BasicJobBuilder setConfig(JobConfig config) {
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

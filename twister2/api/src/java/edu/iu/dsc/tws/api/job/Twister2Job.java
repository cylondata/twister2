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

import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
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
  private WorkerComputeResource requestedResource;
  private int numberOfWorkers;
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
    jobBuilder.setNumberOfWorkers(numberOfWorkers);

    JobAPI.JobResources.Builder jobResourceBuilder = JobAPI.JobResources.newBuilder();

    JobAPI.JobResources.ResourceType.Builder resourceTypeBuilder =
        JobAPI.JobResources.ResourceType.newBuilder();
    resourceTypeBuilder.setNumberOfWorkers(numberOfWorkers);
    resourceTypeBuilder.setWorkerComputeResource(
        JobAPI.WorkerComputeResource.newBuilder()
            .setCpu(requestedResource.getNoOfCpus())
            .setRam(requestedResource.getMemoryMegaBytes())
            .setDisk(requestedResource.getDiskGigaBytes())
            .build()
    );
    jobResourceBuilder.addResources(resourceTypeBuilder.build());

    // set the job resources
    jobBuilder.setJobResources(jobResourceBuilder.build());

    return jobBuilder.build();
  }

  public String getName() {
    return name;
  }

  public String getWorkerClass() {
    return workerClass;
  }

  public WorkerComputeResource getRequestedResource() {
    return requestedResource;
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public JobConfig getConfig() {
    return config;
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

    public BasicJobBuilder setRequestResource(WorkerComputeResource requestResource,
                                              int numberOfWorkers) {
      twister2Job.numberOfWorkers = numberOfWorkers;
      twister2Job.requestedResource = requestResource;
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

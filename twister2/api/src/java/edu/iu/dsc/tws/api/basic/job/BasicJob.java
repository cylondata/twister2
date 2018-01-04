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
package edu.iu.dsc.tws.api.basic.job;

import java.util.Map;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.proto.system.ResourceAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

/**
 * This is a basic job with only communication available
 */
public final class BasicJob {
  private String name;
  private String containerClass;
  private ResourceContainer requestedResource;
  private int noOfContainers;
  private JobConfig config;

  private BasicJob() {
  }

  public JobAPI.Job serialize() {
    JobAPI.Job.Builder jobBuilder = JobAPI.Job.newBuilder();

    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();
    for (Map.Entry<String, String> e : config.entrySet()) {
      JobAPI.Config.KeyValue.Builder keyValueBuilder = JobAPI.Config.KeyValue.newBuilder();
      keyValueBuilder.setKey(e.getKey());
      keyValueBuilder.setValue(e.getValue());
      configBuilder.addKvs(keyValueBuilder);
    }

    jobBuilder.setConfig(configBuilder);

    JobAPI.Container.Builder containerBuilder = JobAPI.Container.newBuilder();
    containerBuilder.setClassName(containerClass);

    jobBuilder.setContainer(containerBuilder);
    jobBuilder.setJobName(name);

    JobAPI.JobResources.Builder jobResourceBuilder = JobAPI.JobResources.newBuilder();
    jobResourceBuilder.setNoOfContainers(noOfContainers);
    ResourceAPI.ComputeResource.Builder computeResourceBuilder =
        ResourceAPI.ComputeResource.newBuilder();
    computeResourceBuilder.setAvailableCPU(requestedResource.getNoOfCpus());
    computeResourceBuilder.setAvailableDisk(requestedResource.getDiskMegaBytes());
    computeResourceBuilder.setAvailableMemory(requestedResource.getMemoryMegaBytes());
    jobResourceBuilder.setContainer(computeResourceBuilder);

    // set the job resources
    jobBuilder.setJobResources(jobResourceBuilder.build());

    return jobBuilder.build();
  }

  public String getName() {
    return name;
  }

  public String getContainerClass() {
    return containerClass;
  }

  public ResourceContainer getRequestedResource() {
    return requestedResource;
  }

  public int getNoOfContainers() {
    return noOfContainers;
  }

  public JobConfig getConfig() {
    return config;
  }

  public static BasicJobBuilder newBuilder() {
    return new BasicJobBuilder();
  }

  public static final class BasicJobBuilder {
    private BasicJob basicJob;

    private BasicJobBuilder() {
      this.basicJob = new BasicJob();
    }

    public BasicJobBuilder setName(String name) {
      basicJob.name = name;
      return this;
    }

    public BasicJobBuilder setContainerClass(String containerClass) {
      basicJob.containerClass = containerClass;
      return this;
    }

    public BasicJobBuilder setRequestResource(ResourceContainer requestResource,
                                              int noOfContainers) {
      basicJob.noOfContainers = noOfContainers;
      basicJob.requestedResource = requestResource;
      return this;
    }

    public BasicJobBuilder setConfig(JobConfig config) {
      basicJob.config = config;
      return this;
    }

    public BasicJob build() {
      if (basicJob.config == null) {
        basicJob.config = new JobConfig();
      }
      return basicJob;
    }
  }
}

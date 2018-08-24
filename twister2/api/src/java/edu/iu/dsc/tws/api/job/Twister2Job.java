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
package edu.iu.dsc.tws.api.job;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.proto.system.ResourceAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This is a basic job with only communication available
 */
public final class Twister2Job {
  private static final Logger LOG = Logger.getLogger(Twister2Job.class.getName());

  private String name;
  private String workerClass;
  private WorkerComputeResource requestedResource;
  private int noOfContainers;
  private JobConfig config;

  private Twister2Job() {
  }

  /**
   * Serializing the JobAPI
   **/
  public JobAPI.Job serialize() {
    JobAPI.Job.Builder jobBuilder = JobAPI.Job.newBuilder();

    JobAPI.Config.Builder configBuilder = JobAPI.Config.newBuilder();

    Set<Map.Entry<String, Object>> configEntry = config.entrySet();
    Set<Map.Entry<String, byte[]>> configByteEntry = new HashSet<>();
    KryoSerializer kryoSerializer = new KryoSerializer();
    for (Map.Entry<String, Object> e : configEntry) {
      String key = e.getKey();
      Object object = e.getValue();
      byte[] objectByte = kryoSerializer.serialize(object);
      Map.Entry<String, byte[]> entry =
          new AbstractMap.SimpleEntry<String, byte[]>(key, objectByte);
      configByteEntry.add(entry);
    }

    for (Map.Entry<String, byte[]> e : configByteEntry) {
      String key = e.getKey();
      byte[] bytes = e.getValue();
      ByteString byteString = ByteString.copyFrom(bytes);
      configBuilder.putConfigByteMap(key, byteString);
    }

    jobBuilder.setConfig(configBuilder);
    jobBuilder.setWorkerClassName(workerClass);
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

  public String getWorkerClass() {
    return workerClass;
  }

  public WorkerComputeResource getRequestedResource() {
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
                                              int noOfContainers) {
      twister2Job.noOfContainers = noOfContainers;
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

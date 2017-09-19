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

import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResource;

public final class BasicJob {
  private String name;
  private String containerClass;
  private RequestedResource requestedResource;
  private int noOfContainers;

  private BasicJob() {
  }

  public JobAPI.Job serialize() {
    return null;
  }

  public BasicJobBuilder newBuilder() {
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

    public BasicJobBuilder setRequestResource(RequestedResource requestResource,
                                              int noOfContainers) {
      basicJob.noOfContainers = noOfContainers;
      basicJob.requestedResource = requestResource;
      return this;
    }

    public BasicJob build() {
      return basicJob;
    }
  }
}

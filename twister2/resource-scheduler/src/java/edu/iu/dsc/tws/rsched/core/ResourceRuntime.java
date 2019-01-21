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
package edu.iu.dsc.tws.rsched.core;

public final class ResourceRuntime {
  private static ResourceRuntime ourInstance = new ResourceRuntime();

  private String jobMasterHost = null;

  private int jobMasterPort = -1;

  public static ResourceRuntime getInstance() {
    return ourInstance;
  }

  private ResourceRuntime() {
  }

  public void setJobMasterHostPort(String host, int port) {
    this.jobMasterHost = host;
    this.jobMasterPort = port;
  }

  public String getJobMasterHost() {
    return jobMasterHost;
  }

  public int getJobMasterPort() {
    return jobMasterPort;
  }
}

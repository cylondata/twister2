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
package edu.iu.dsc.tws.api.htgjob;

import java.util.List;

import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public final class Twister2HTGInstance {

  private HTGJobAPI.HTGJob htgJob;
  private Twister2HTGScheduler twister2HTGScheduler;
  private List<HTGJobAPI.ExecuteMessage> executeMessagesList;

  public String getHtgSchedulerClassName() {
    return htgSchedulerClassName;
  }

  public void setHtgSchedulerClassName(String htgSchedulerClassName) {
    this.htgSchedulerClassName = htgSchedulerClassName;
  }

  private String htgSchedulerClassName;

  private static Twister2HTGInstance singletonHTGInstance = new Twister2HTGInstance();

  public HTGJobAPI.HTGJob getHtgJob() {
    return htgJob;
  }

  public void setHtgJob(HTGJobAPI.HTGJob htgJob) {
    this.htgJob = htgJob;
  }

  public Twister2HTGScheduler getTwister2HTGScheduler() {
    return twister2HTGScheduler;
  }

  public void setTwister2HTGScheduler(Twister2HTGScheduler twister2HTGScheduler) {
    this.twister2HTGScheduler = twister2HTGScheduler;
  }

  public List<HTGJobAPI.ExecuteMessage> getExecuteMessagesList() {
    return executeMessagesList;
  }

  public void setExecuteMessagesList(List<HTGJobAPI.ExecuteMessage> executeMessagesList) {
    this.executeMessagesList = executeMessagesList;
  }

  private Twister2HTGInstance() {
  }

  public static Twister2HTGInstance getTwister2HTGInstance() {
    return singletonHTGInstance;
  }

}

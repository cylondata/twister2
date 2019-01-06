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
package edu.iu.dsc.tws.api.cdfw;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public final class Twister2HTGInstance {
  private DefaultScheduler defaultScheduler;

  private List<HTGJobAPI.ExecuteMessage> executeMessagesList;

  private String htgSchedulerClassName;

  private static Twister2HTGInstance singletonHTGInstance = new Twister2HTGInstance();

  private BlockingQueue<DriverEvent> driverEvents;

  private Twister2HTGInstance() {
  }

  public String getHtgSchedulerClassName() {
    return htgSchedulerClassName;
  }

  public void setHtgSchedulerClassName(String htgSchedulerClassName) {
    this.htgSchedulerClassName = htgSchedulerClassName;
  }

  public DefaultScheduler getDefaultScheduler() {
    return defaultScheduler;
  }

  public void setDefaultScheduler(DefaultScheduler defaultScheduler) {
    this.defaultScheduler = defaultScheduler;
  }

  public List<HTGJobAPI.ExecuteMessage> getExecuteMessagesList() {
    return executeMessagesList;
  }

  public void setExecuteMessagesList(List<HTGJobAPI.ExecuteMessage> executeMessagesList) {
    this.executeMessagesList = executeMessagesList;
  }

  public static Twister2HTGInstance getTwister2HTGInstance() {
    return singletonHTGInstance;
  }

  public void setDriverEvents(BlockingQueue<DriverEvent> events) {
    if (driverEvents != null) {
      throw new RuntimeException("Cannot set the queue twice");
    }
    this.driverEvents = events;
  }
}

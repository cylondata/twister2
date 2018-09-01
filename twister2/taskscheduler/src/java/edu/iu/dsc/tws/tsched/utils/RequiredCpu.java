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
package edu.iu.dsc.tws.tsched.utils;

/**
 * This class has getters and setters property to get and set the required cpu values for the
 * task instances.
 */
public class RequiredCpu {

  private String taskName;
  private Double requiredCpu;

  public RequiredCpu(String taskName, Double requiredCpu) {
    this.taskName = taskName;
    this.requiredCpu = requiredCpu;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequiredCpu)) {
      return false;
    }

    RequiredCpu that = (RequiredCpu) o;

    if (!taskName.equals(that.taskName)) {
      return false;
    }
    return requiredCpu.equals(that.requiredCpu);
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + requiredCpu.hashCode();
    return result;
  }

  public Double getRequiredCpu() {
    return requiredCpu;
  }

  public void setRequiredCpu(Double requiredCpu) {
    this.requiredCpu = requiredCpu;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }


}


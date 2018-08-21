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
 * This class has getters and setters property to get and set the required disk values for the
 * task instances.
 */
public class RequiredDisk {

  private String taskName;
  private Double requiredDisk;

  public RequiredDisk(String taskName, Double disk) {
    this.taskName = taskName;
    this.requiredDisk = disk;
  }

  public Double getRequiredDisk() {
    return requiredDisk;
  }

  public void setRequiredDisk(Double requiredDisk) {
    this.requiredDisk = requiredDisk;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequiredDisk)) {
      return false;
    }

    RequiredDisk that = (RequiredDisk) o;

    if (!taskName.equals(that.taskName)) {
      return false;
    }
    return requiredDisk.equals(that.requiredDisk);
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + requiredDisk.hashCode();
    return result;
  }
}


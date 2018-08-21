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
 * This class has getters and setters property to get and set the required ram values for the
 * task instances.
 */
public class RequiredRam implements Comparable<RequiredRam> {

  private String taskName;
  private Double requiredRam;

  public RequiredRam(String taskName, Double ram) {
    this.taskName = taskName;
    this.requiredRam = ram;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequiredRam)) {
      return false;
    }

    RequiredRam that = (RequiredRam) o;
    if (!taskName.equals(that.taskName)) {
      return false;
    }
    return requiredRam.equals(that.requiredRam);
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + requiredRam.hashCode();
    return result;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public Double getRequiredRam() {
    return requiredRam;
  }

  public void setRequiredRam(Double requiredRam) {
    this.requiredRam = requiredRam;
  }

  @Override
  public int compareTo(RequiredRam o) {
    return this.requiredRam.compareTo(o.requiredRam);
  }
}


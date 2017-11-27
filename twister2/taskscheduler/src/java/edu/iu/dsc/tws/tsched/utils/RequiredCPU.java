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

public class RequiredCPU {

  private String taskName;
  private Double requiredCPU;

  public RequiredCPU(String taskName, Double CPU) {
    this.taskName = taskName;
    this.requiredCPU = CPU;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public Double getRequiredCPU() {
    return requiredCPU;
  }

  public void setRequiredCPU(Double requiredCPU) {
    this.requiredCPU = requiredCPU;
  }

  public String getTaskName() {
    return taskName;
  }

  public int compareTo(RequiredCPU requiredCPU) {
    return this.requiredCPU.compareTo(requiredCPU.requiredCPU);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RequiredCPU)) return false;

    RequiredCPU that = (RequiredCPU) o;

    if (!taskName.equals(that.taskName)) return false;
    return requiredCPU.equals(that.requiredCPU);
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + requiredCPU.hashCode();
    return result;
  }

}

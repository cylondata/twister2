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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

public class JobSubmissionStatus {
  private boolean serviceForWorkersCreated;
  private boolean serviceForJobMasterCreated;
  private boolean statefulsetForWorkersCreated;
  private boolean statefulsetForJobMasterCreated;
  private boolean pvcCreated;

  public JobSubmissionStatus() { }

  public boolean isServiceForWorkersCreated() {
    return serviceForWorkersCreated;
  }

  public boolean isServiceForJobMasterCreated() {
    return serviceForJobMasterCreated;
  }

  public boolean isStatefulsetForWorkersCreated() {
    return statefulsetForWorkersCreated;
  }

  public boolean isStatefulsetForJobMasterCreated() {
    return statefulsetForJobMasterCreated;
  }

  public boolean isPvcCreated() {
    return pvcCreated;
  }

  public void setServiceForWorkersCreated(boolean serviceForWorkersCreated) {
    this.serviceForWorkersCreated = serviceForWorkersCreated;
  }

  public void setServiceForJobMasterCreated(boolean serviceForJobMasterCreated) {
    this.serviceForJobMasterCreated = serviceForJobMasterCreated;
  }

  public void setStatefulsetForWorkersCreated(boolean statefulsetForWorkersCreated) {
    this.statefulsetForWorkersCreated = statefulsetForWorkersCreated;
  }

  public void setStatefulsetForJobMasterCreated(boolean statefulsetForJobMasterCreated) {
    this.statefulsetForJobMasterCreated = statefulsetForJobMasterCreated;
  }

  public void setPvcCreated(boolean pvcCreated) {
    this.pvcCreated = pvcCreated;
  }
}

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

//This class will be replaced with the original job config file from the job package

public class JobConfig {

  public static String numberOfContainers = "2";
  public static String numberOfInstances = "4";
  public static String containerMaxRAMValue = "24";
  public static String containerMaxCpuValue = "0.2";
  public static String containerMaxDiskValue = "20";

  public int numberOfWorkers = 3;
  public double componentRam = 20;
  public double componentDisk = 10;
  public double componentCPU = 0.6;

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }

  public double getComponentRam() {
    return componentRam;
  }

  public void setComponentRam(double componentRam) {
    this.componentRam = componentRam;
  }

  public double getComponentDisk() {
    return componentDisk;
  }

  public void setComponentDisk(double componentDisk) {
    this.componentDisk = componentDisk;
  }

  public double getComponentCPU() {
    return componentCPU;
  }

  public void setComponentCPU(double componentCPU) {
    this.componentCPU = componentCPU;
  }

}

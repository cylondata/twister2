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
 * This class has getters and setters property to set the data transfer time for the nodes.
 */
public class DataTransferTimeCalculator implements Comparable<DataTransferTimeCalculator> {

  private String nodeName;

  private Double dataTransferTime;

  private String dataNode;

  private int taskIndex;

  public DataTransferTimeCalculator(String nodename,
                                    double dataTransferTime, String datanode) {
    this.nodeName = nodename;
    this.dataTransferTime = dataTransferTime;
    this.dataNode = datanode;
  }

  public DataTransferTimeCalculator setNodeName(String nodename) {
    this.nodeName = nodename;
    return this;
  }

  public DataTransferTimeCalculator(String nodename, double requiredDatatransferTime) {
    this.nodeName = nodename;
    this.dataTransferTime = requiredDatatransferTime;
  }

  public String getDataNode() {
    return dataNode;
  }

  public DataTransferTimeCalculator setDataNode(String datanode) {
    this.dataNode = datanode;
    return this;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public DataTransferTimeCalculator setTaskIndex(int taskindex) {
    this.taskIndex = taskindex;
    return this;
  }

  public String getNodeName() {
    return nodeName;
  }

  public Double getRequiredDataTransferTime() {
    return dataTransferTime;
  }

  public DataTransferTimeCalculator setRequiredDataTransferTime(Double requiredDatatransferTime) {
    this.dataTransferTime = requiredDatatransferTime;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataTransferTimeCalculator)) {
      return false;
    }

    DataTransferTimeCalculator that = (DataTransferTimeCalculator) o;

    return dataTransferTime.equals(that.dataTransferTime);
  }

  @Override
  public int hashCode() {
    return dataTransferTime.hashCode();
  }

  @Override
  public int compareTo(DataTransferTimeCalculator o) {
    return this.getRequiredDataTransferTime().compareTo(o.getRequiredDataTransferTime());
  }
}


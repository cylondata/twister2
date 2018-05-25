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

public class CalculateDataTransferTime implements Comparable<CalculateDataTransferTime> {

  public CalculateDataTransferTime setNodeName(String nodename) {
    this.nodeName = nodename;
    return this;
  }

  private String nodeName;
  private Double calculateDataTransferTime;

  private String dataNode;

  public CalculateDataTransferTime(String nodename,
                                   double requiredDatatransferTime, String datanode) {
    this.nodeName = nodename;
    this.calculateDataTransferTime = requiredDatatransferTime;
    this.dataNode = datanode;
  }

  public String getDataNode() {
    return dataNode;
  }


  public int getTaskIndex() {
    return taskIndex;
  }

  public CalculateDataTransferTime setTaskIndex(int taskindex) {
    this.taskIndex = taskindex;
    return this;
  }

  private int taskIndex;

  public CalculateDataTransferTime(String nodename, double requiredDatatransferTime) {
    this.nodeName = nodename;
    this.calculateDataTransferTime = requiredDatatransferTime;
  }

  public CalculateDataTransferTime setDataNode(String datanode) {
    this.dataNode = datanode;
    return this;
  }

  public String getNodeName() {
    return nodeName;
  }

  public Double getRequiredDataTransferTime() {
    return calculateDataTransferTime;
  }

  public CalculateDataTransferTime setRequiredDataTransferTime(Double requiredDatatransferTime) {
    this.calculateDataTransferTime = requiredDatatransferTime;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CalculateDataTransferTime)) {
      return false;
    }

    CalculateDataTransferTime that = (CalculateDataTransferTime) o;

    return calculateDataTransferTime.equals(that.calculateDataTransferTime);
  }

  @Override
  public int hashCode() {
    return calculateDataTransferTime.hashCode();
  }

  @Override
  public int compareTo(CalculateDataTransferTime o) {
    return this.getRequiredDataTransferTime().compareTo(o.getRequiredDataTransferTime());
  }
}


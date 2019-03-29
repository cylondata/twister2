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
package org.apache.storm.topology.twister2;

import java.io.Serializable;

import org.apache.storm.tuple.Fields;

public class Twister2BoltGrouping implements Serializable {

  private GroupingTechnique groupingTechnique;
  private String componentId;
  private String streamId;

  private Fields groupingKey;

  public Fields getGroupingKey() {
    return groupingKey;
  }

  public void setGroupingKey(Fields groupingKey) {
    this.groupingKey = groupingKey;
  }

  public GroupingTechnique getGroupingTechnique() {
    return groupingTechnique;
  }

  public void setGroupingTechnique(GroupingTechnique groupingTechnique) {
    this.groupingTechnique = groupingTechnique;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  public String getStreamId() {
    return streamId;
  }

  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  @Override
  public String toString() {
    return "Twister2BoltGrouping{"
        + "groupingTechnique=" + groupingTechnique
        + ", componentId='" + componentId + '\''
        + ", streamId='" + streamId + '\''
        + ", groupingKey=" + groupingKey
        + '}';
  }
}

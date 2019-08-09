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
package edu.iu.dsc.tws.task.impl.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeConnectionUtils;
import edu.iu.dsc.tws.task.impl.TaskConfigurations;

public abstract class AbstractOpsConfig<T extends AbstractOpsConfig> {

  private static final Logger LOG = Logger.getLogger(AbstractOpsConfig.class.getName());

  protected String source;
  protected String operationName;
  protected ComputeConnection computeConnection;
  protected String edgeName = TaskConfigurations.DEFAULT_EDGE;
  protected MessageType opDataType = MessageTypes.OBJECT;
  protected Map<String, Object> propertiesMap;
  protected MessageSchema messageSchema = MessageSchema.noSchema();

  protected AbstractOpsConfig(String source,
                              String operationName,
                              ComputeConnection computeConnection) {
    this.source = source;
    this.operationName = operationName;
    this.computeConnection = computeConnection;
    this.propertiesMap = new HashMap<>();
  }

  public T viaEdge(String edge) {
    this.edgeName = edge;
    return (T) this;
  }

  public T withProperties(Map<String, Object> properties) {
    this.propertiesMap.putAll(properties);
    return (T) this;
  }

  public T withProperty(String propertyName, Object property) {
    this.propertiesMap.put(propertyName, property);
    return (T) this;
  }

  public T withDataType(MessageType dataType) {
    this.opDataType = dataType;
    return (T) this;
  }

  public T withMessageSchema(MessageSchema messageSchema) {
    this.messageSchema = messageSchema;
    return (T) this;
  }

  public String getSource() {
    return source;
  }

  public String getEdgeName() {
    return edgeName;
  }

  protected MessageType getOpDataType() {
    return opDataType;
  }

  abstract void validate();

  protected void runValidation() {
    if (this.source == null) {
      throw new OpConfigValidationFailedException("Parent can't be null");
    }
    this.validate();
  }

  protected static void failValidation(String msg) {
    throw new OpConfigValidationFailedException(msg);
  }

  protected abstract Edge updateEdge(Edge newEdge);

  public ComputeConnection connect() {
    ComputeConnectionUtils.connectEdge(this.computeConnection, this.source, this.buildEdge());
    return this.computeConnection;
  }

  Edge buildEdge() {
    this.runValidation();
    Edge edge = new Edge(this.edgeName, this.operationName);
    edge.setDataType(opDataType);
    edge.addProperties(propertiesMap);
    edge.setMessageSchema(messageSchema);
    return this.updateEdge(edge);
  }
}

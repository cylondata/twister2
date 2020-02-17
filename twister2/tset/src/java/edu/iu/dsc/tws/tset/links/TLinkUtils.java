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
package edu.iu.dsc.tws.tset.links;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;

public final class TLinkUtils {

  private TLinkUtils() {

  }

  public static void generateCommsSchema(Schema schema, Edge edge) {
    if (schema.isLengthsSpecified()) {
      edge.setMessageSchema(MessageSchema.ofSize(schema.getTotalSize()));
    }
  }

  public static void generateKeyedCommsSchema(TupleSchema schema, Edge edge) {
    if (schema.isLengthsSpecified()) {
      edge.setMessageSchema(MessageSchema.ofSize(schema.getTotalSize(), schema.getKeySize()));
    }
  }

  public static void generateCommsSchema(Edge edge) {
    if (!edge.getMessageSchema().isFixedSchema()) {
      MessageType keyType = edge.getKeyType();
      MessageType dataType = edge.getDataType();
      if (edge.isKeyed()) {
        if (keyType.isPrimitive() && dataType.isPrimitive()
            && !keyType.isArray() && !dataType.isArray()) {
          edge.setMessageSchema(MessageSchema.ofSize(
              keyType.getUnitSizeInBytes() + dataType.getUnitSizeInBytes(),
              keyType.getUnitSizeInBytes()
          ));
        }
      } else if (dataType.isPrimitive() && !dataType.isArray()) {
        edge.setMessageSchema(MessageSchema.ofSize(dataType.getUnitSizeInBytes()));
      }
    }
  }
}

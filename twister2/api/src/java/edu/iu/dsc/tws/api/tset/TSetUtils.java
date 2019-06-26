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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedGatherTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;

public final class TSetUtils {

  private TSetUtils() {
  }

  public static <T> boolean isKeyedInput(BaseTLink<T> parent) {
    return parent instanceof KeyedGatherTLink || parent instanceof KeyedReduceTLink
        || parent instanceof KeyedPartitionTLink;
  }

  /**
   * Check if the link is Iterable
   */
  public static <T> boolean isIterableInput(BaseTLink<T> parent, OperationMode mode) {
    if (mode == OperationMode.STREAMING) {
      if (parent instanceof DirectTLink) {
        return true;
      } else if (parent instanceof ReduceTLink) {
        return false;
      } else if (parent instanceof KeyedReduceTLink) {
        return false;
      } else if (parent instanceof GatherTLink || parent instanceof KeyedGatherTLink) {
        return true;
      } else if (parent instanceof AllReduceTLink) {
        return false;
      } else if (parent instanceof AllGatherTLink) {
        return true;
      } else if (parent instanceof PartitionTLink || parent instanceof KeyedPartitionTLink) {
        return true;
      } else if (parent instanceof ReplicateTLink) {
        return false;
      } else {
        throw new RuntimeException("Failed to build un-supported operation: " + parent);
      }
    } else {
      if (parent instanceof DirectTLink) {
        return true;
      } else if (parent instanceof ReduceTLink) {
        return false;
      } else if (parent instanceof KeyedReduceTLink) {
        return true;
      } else if (parent instanceof GatherTLink || parent instanceof KeyedGatherTLink) {
        return true;
      } else if (parent instanceof AllReduceTLink) {
        return false;
      } else if (parent instanceof AllGatherTLink) {
        return true;
      } else if (parent instanceof PartitionTLink || parent instanceof KeyedPartitionTLink) {
        return true;
      } else if (parent instanceof ReplicateTLink) {
        return true;
      } else {
        throw new RuntimeException("Failed to build un-supported operation: " + parent);
      }
    }
  }

  public static MessageType getDataType(Class type) {
    if (type == int[].class) {
      return MessageTypes.INTEGER_ARRAY;
    } else if (type == double[].class) {
      return MessageTypes.DOUBLE_ARRAY;
    } else if (type == short[].class) {
      return MessageTypes.SHORT_ARRAY;
    } else if (type == byte[].class) {
      return MessageTypes.BYTE_ARRAY;
    } else if (type == long[].class) {
      return MessageTypes.LONG_ARRAY;
    } else if (type == char[].class) {
      return MessageTypes.CHAR_ARRAY;
    } else {
      return MessageTypes.OBJECT;
    }
  }

  public static MessageType getKeyType(Class type) {
    if (type == Integer.class) {
      return MessageTypes.INTEGER_ARRAY;
    } else if (type == Double.class) {
      return MessageTypes.DOUBLE_ARRAY;
    } else if (type == Short.class) {
      return MessageTypes.SHORT_ARRAY;
    } else if (type == Byte.class) {
      return MessageTypes.BYTE_ARRAY;
    } else if (type == Long.class) {
      return MessageTypes.LONG_ARRAY;
    } else if (type == Character.class) {
      return MessageTypes.CHAR_ARRAY;
    } else {
      return MessageTypes.OBJECT;
    }
  }
}

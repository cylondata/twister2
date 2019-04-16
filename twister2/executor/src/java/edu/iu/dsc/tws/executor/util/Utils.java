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
package edu.iu.dsc.tws.executor.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.data.api.DataType;

public final class Utils {
  private Utils() {
  }

  public static MessageType dataTypeToMessageType(DataType type) {
    switch (type) {
      case OBJECT:
        return MessageTypes.OBJECT;
      case BYTE:
        return MessageTypes.BYTE;
      case BYTE_ARRAY:
        return MessageTypes.BYTE_ARRAY;
      case INTEGER:
        return MessageTypes.INTEGER;
      case INTEGER_ARRAY:
        return MessageTypes.INTEGER_ARRAY;
      case DOUBLE:
        return MessageTypes.DOUBLE;
      case DOUBLE_ARRAY:
        return MessageTypes.DOUBLE_ARRAY;
      case CHAR:
        return MessageTypes.CHAR;
      case CHAR_ARRAY:
        return MessageTypes.CHAR_ARRAY;
      case LONG:
        return MessageTypes.LONG;
      case LONG_ARRAY:
        return MessageTypes.LONG_ARRAY;
      case SHORT:
        return MessageTypes.SHORT;
      case SHORT_ARRAY:
        return MessageTypes.SHORT_ARRAY;
      default:
        throw new RuntimeException("Un-expected type");
    }
  }

  public static byte[] serialize(Object obj) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.close();
      return bos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static Object deserialize(byte[] serialized) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
      ObjectInputStream ois = new ObjectInputStream(bis);
      Object ret = ois.readObject();
      ois.close();
      return ret;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

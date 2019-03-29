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
import edu.iu.dsc.tws.data.api.DataType;

public final class Utils {
  private Utils() {
  }

  public static MessageType dataTypeToMessageType(DataType type) {
    switch (type) {
      case OBJECT:
        return MessageType.OBJECT;
      case BYTE:
        return MessageType.BYTE;
      case INTEGER:
        return MessageType.INTEGER;
      case DOUBLE:
        return MessageType.DOUBLE;
      case CHAR:
        return MessageType.CHAR;
      case LONG:
        return MessageType.LONG;
      case SHORT:
        return MessageType.SHORT;
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

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
package edu.iu.dsc.tws.common.net.tcp.request;

import java.util.Arrays;
import java.util.Random;

/**
 * A Request id generator
 */
public final class RequestID {
  private static Random randomGenerator = new Random(System.nanoTime());

  public static final int ID_SIZE = 32;

  private byte[] id;

  private RequestID(byte[] id) {
    this.id = id;
  }

  public static RequestID fromBytes(byte[] bytes) {
    return new RequestID(bytes);
  }

  public static RequestID generate() {
    byte[] dataBytes = new byte[ID_SIZE];
    randomGenerator.nextBytes(dataBytes);
    return new RequestID(dataBytes);
  }

  public byte[] getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RequestID requestID = (RequestID) o;
    return Arrays.equals(id, requestID.id);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(id);
  }
}

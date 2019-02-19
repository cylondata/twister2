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
package edu.iu.dsc.tws.comms.dfw.io.types;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.KeyPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

public class IntegerKeyPacker implements KeyPacker {

  public IntegerKeyPacker() {
  }

  @Override
  public int packKey(Object key, SerializeState state) {
    return Integer.BYTES;
  }

  @Override
  public boolean writeKeyToBuffer(Object key,
                                  ByteBuffer targetBuffer, SerializeState state) {
    return KeySerializer.copyKeyToBuffer(key, MessageType.INTEGER, targetBuffer,
        state, null);
  }

  @Override
  public Pair<Integer, Integer> getKeyLength(InMessage message,
                                             DataBuffer buffer, int location) {
    return PartialKeyDeSerializer.createKey(message, buffer, location);
  }

  @Override
  public int readKeyFromBuffer(InMessage currentMessage, int currentLocation,
                               DataBuffer buffer, int currentObjectLength) {
    return PartialKeyDeSerializer.readFromBuffer(currentMessage, currentLocation, buffer,
        currentObjectLength, null);
  }

  @Override
  public Object initializeUnPackKeyObject(int size) {
    return PartialKeyDeSerializer.createKeyObject(MessageType.INTEGER, size);
  }

  @Override
  public boolean isKeyHeaderRequired() {
    return false;
  }
}

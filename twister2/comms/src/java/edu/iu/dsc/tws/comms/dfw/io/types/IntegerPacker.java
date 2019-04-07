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

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.KeyPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

public final class IntegerPacker implements KeyPacker<Integer>, DataPacker<Integer> {

  private static volatile IntegerPacker instance;

  private IntegerPacker() {
  }

  public static IntegerPacker getInstance() {
    if (instance == null) {
      instance = new IntegerPacker();
    }
    return instance;
  }

  @Override
  public int packKey(Integer key, SerializeState state) {
    return this.packData(key, state);
  }

  @Override
  public boolean writeKeyToBuffer(Integer key,
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
    return PartialKeyDeSerializer.readFromBuffer(currentMessage, MessageType.INTEGER,
        currentLocation, buffer, currentObjectLength, null);
  }

  @Override
  public Integer initializeUnPackKeyObject(int size) {
    return null;
  }

  @Override
  public boolean isKeyHeaderRequired() {
    return false;
  }

  @Override
  public int packData(Integer data, SerializeState state) {
    return Integer.BYTES;
  }

  @Override
  public boolean writeDataToBuffer(Integer data, ByteBuffer targetBuffer, SerializeState state) {
    return false;
  }

  @Override
  public Integer initializeUnPackDataObject(int length) {
    return null;
  }

  @Override
  public int readDataFromBuffer(InMessage currentMessage,
                                int currentLocation, DataBuffer buffer,
                                int currentObjectLength) {
    return 0;
  }
}

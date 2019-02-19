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
package edu.iu.dsc.tws.comms.api;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

/**
 * The data packer interface. An implementation class should be stateless.
 */
public interface DataPacker {

  /**
   * Pack the data and return the size of the data in bytes once packed
   *
   * @param data the data (can be Integer, Object etc)
   * @param state state
   * @return the size of the packed data in bytes
   */
  int packData(Object data, SerializeState state);


  /**
   * Transfer the data to the buffer
   * @param data the data
   * @param targetBuffer target buffer
   * @param state this can be used to keep the sate about the packing object
   * @return true if all the data is packed
   */
  boolean writeDataToBuffer(Object data,
                        ByteBuffer targetBuffer, SerializeState state);

  /**
   * Initialize the object that we are going to create
   * @param length byte length
   * @return the object created
   */
  Object initializeUnPackDataObject(int length);

  /**
   * Read the data from the buffer
   * @param currentMessage the current message
   * @param currentLocation current location
   * @param buffer buffer
   * @param currentObjectLength the current object length
   * @return the number of bytes read
   */
  int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                         DataBuffer buffer, int currentObjectLength);
}


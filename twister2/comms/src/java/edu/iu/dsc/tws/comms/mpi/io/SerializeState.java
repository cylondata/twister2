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
package edu.iu.dsc.tws.comms.mpi.io;

/**
 * Keep track of the serialization state of the list of objects
 */
public class SerializeState {
  // the current object
  private int currentObject;
  // bytes copied of the current object
  private int bytesCopied;
  // buffer no we are working on
  private int bufferNo;
  // the serialized data of the current object
  private byte[] data;

  public int getCurrentObject() {
    return currentObject;
  }

  public int getBytesCopied() {
    return bytesCopied;
  }

  public int getBufferNo() {
    return bufferNo;
  }

  public void setCurrentObject(int currentObject) {
    this.currentObject = currentObject;
  }

  public void setBytesCopied(int bytesCopied) {
    this.bytesCopied = bytesCopied;
  }

  public void setBufferNo(int bufferNo) {
    this.bufferNo = bufferNo;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }
}

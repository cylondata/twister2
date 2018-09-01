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
package edu.iu.dsc.tws.comms.dfw.io;

/**
 * Keep track of the serialization state of the list of objects
 */
public class SerializeState {
  // the current object
  private int currentObject;
  // bytes copied of the current object
  private int bytesCopied;
  // buffer no we are working on, this is for MPI Messages
  private int bufferNo;
  // the serialized data of the current object
  private byte[] data;
  // the total bytes, including the length and task for each message
  private int totalBytes;
  // length header built
  private boolean headerBuilt;
  // the serialized key
  private byte[] key;
  // length of the serialized key
  private int keySize;
  //The size that should be set as the current header length
  private int curretHeaderLength;

  public enum Part {
    INIT,
    HEADER,
    BODY,
  }

  // which part of the message is been serialized
  private Part part = Part.INIT;

  public int getTotalBytes() {
    return totalBytes;
  }

  public void setTotalBytes(int totalBytes) {
    this.totalBytes = totalBytes;
  }

  public int addTotalBytes(int bytes) {
    totalBytes += bytes;
    return totalBytes;
  }

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

  public boolean isHeaderBuilt() {
    return headerBuilt;
  }

  public void setHeaderBuilt(boolean headerBuilt) {
    this.headerBuilt = headerBuilt;
  }

  public byte[] getKey() {
    return key;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public Part getPart() {
    return part;
  }

  public void setPart(Part part) {
    this.part = part;
  }

  public int getKeySize() {
    return keySize;
  }

  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  public int getCurretHeaderLength() {
    return curretHeaderLength;
  }

  public void setCurretHeaderLength(int curretHeaderLength) {
    this.curretHeaderLength = curretHeaderLength;
  }
}

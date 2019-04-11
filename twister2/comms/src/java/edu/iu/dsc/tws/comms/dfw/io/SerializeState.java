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

import edu.iu.dsc.tws.comms.api.PackerStore;

/**
 * Keep track of the serialization state of the list of objects.
 */
public class SerializeState extends PackerStore {
  // the current object index
  private int currentObjectIndex;

  // buffer no we are working on, this is for MPI Messages
  private int bufferNo;

  // the total bytes, including the length and task for each message
  private int totalBytes;

  //The size that should be set as the current header length
  private int currentHeaderLength;

  public static class StoredData {
    private byte[] data;
    private int bytesCopied;
    private int totalToCopy;

    private void clear() {
      this.data = null;
      this.bytesCopied = 0;
    }

    public int getBytesCopied() {
      return bytesCopied;
    }

    public void setTotalToCopy(int totalToCopy) {
      if (data != null && data.length != totalToCopy) {
        throw new RuntimeException(
            "Assertion failed. Total to copy is different than data length."
                + "Expected : " + data.length + ", Found : " + totalToCopy
        );
      }
      this.totalToCopy = totalToCopy;
    }

    public int leftToCopy() {
      return this.totalToCopy - this.bytesCopied;
    }

    public boolean hasCompleted() {
      if (bytesCopied > totalToCopy) {
        throw new RuntimeException(
            "Assertion failed. Packer has copied more data than expected."
                + "Expected : " + totalToCopy + ", Found : " + bytesCopied
        );
      }
      return this.bytesCopied == this.totalToCopy;
    }

    public void incrementCopied(int by) {
      this.bytesCopied += by;
    }
  }

  // Below objects will store either key or value. We need only two since we have AKeyed and Keyed
  // messages only.
  private StoredData storedData1 = new StoredData();
  private StoredData storedData2 = new StoredData();

  private StoredData active = storedData1;

  @Override
  public void store(byte[] data) {
    this.active.clear();
    this.active.data = data;
  }

  @Override
  public byte[] retrieve() {
    return this.active.data;
  }

  /**
   * Swaps active {@link StoredData} instance
   */
  void swap() {
    if (active == storedData1) {
      active = storedData2;
    } else {
      active = storedData1;
    }
  }

  void clearActive() {
    this.active.clear();
  }

  StoredData getActive() {
    return this.active;
  }

  // the state of each part
  public enum Part {
    INIT,
    HEADER,
    KEY,
    BODY,
  }

  // which part of the message is been serialized
  private Part part = Part.INIT;

  void incrementTotalBytes(int by) {
    this.totalBytes += by;
  }

  public int getTotalBytes() {
    return totalBytes;
  }

  public int getCurrentObjectIndex() {
    return currentObjectIndex;
  }

  public int getBufferNo() {
    return bufferNo;
  }

  public void setCurrentObjectIndex(int currentObjectIndex) {
    this.currentObjectIndex = currentObjectIndex;
  }

  public void setBufferNo(int bufferNo) {
    this.bufferNo = bufferNo;
  }

  public Part getPart() {
    return part;
  }

  public void setPart(Part part) {
    this.part = part;
  }

  public int getCurrentHeaderLength() {
    return currentHeaderLength;
  }

  public void setCurrentHeaderLength(int currentHeaderLength) {
    this.currentHeaderLength = currentHeaderLength;
  }

  public boolean reset(boolean completed) {
    if (completed) {
      this.setBufferNo(0);
      this.setPart(SerializeState.Part.INIT);
      this.storedData1.clear();
      this.storedData2.clear();
      this.clear();
      return true;
    } else {
      return false;
    }
  }
}

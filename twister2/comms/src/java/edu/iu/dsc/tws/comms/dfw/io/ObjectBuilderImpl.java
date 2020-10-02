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

import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.ObjectBuilder;

public class ObjectBuilderImpl<D, W> implements ObjectBuilder<D, W> {

  private boolean built;

  private W partialDataHolder;
  private D finalObject;

  private int totalSize;
  private int completedSize;

  void init(DataPacker<D, W> dataPacker, int totSize) {
    this.reset();
    this.partialDataHolder = dataPacker.wrapperForByteLength(totSize);
    this.totalSize = totSize;
  }

  void incrementCompletedSizeBy(int bytes) {
    this.completedSize += bytes;
  }

  public int getCompletedSize() {
    return completedSize;
  }

  public int getTotalSize() {
    return totalSize;
  }

  boolean isBuilt() {
    return built;
  }

  private void markBuilt() {
    this.built = true;
  }

  public W getPartialDataHolder() {
    return partialDataHolder;
  }

  public void setFinalObject(D finalObject) {
    this.finalObject = finalObject;
    this.markBuilt();
  }

  public D getFinalObject() {
    return finalObject;
  }

  private void reset() {
    this.partialDataHolder = null;
    this.totalSize = 0;
    this.finalObject = null;
    this.built = false;
    this.completedSize = 0;
  }
}

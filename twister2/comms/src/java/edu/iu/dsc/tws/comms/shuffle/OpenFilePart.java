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
package edu.iu.dsc.tws.comms.shuffle;

import java.util.List;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;

/**
 * Represent an open file part.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OpenFilePart {
  // the key values read
  private List<Tuple> keyValues;

  // the current read offset
  private long readOffSet;

  // size of the file
  private long fileSize;

  // name of te file
  private String fileName;

  // the current read index of key values
  private int keyValueIndex = 0;

  OpenFilePart(List<Tuple> keyValues, long readOffSet, long fileSize, String fileName) {
    this.keyValues = keyValues;
    this.readOffSet = readOffSet;
    this.fileSize = fileSize;
    this.fileName = fileName;
  }

  public long getReadOffSet() {
    return readOffSet;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String getFileName() {
    return fileName;
  }

  public Tuple next() {
    Tuple kv = keyValues.get(keyValueIndex);
    keyValueIndex++;
    return kv;
  }

  public boolean hasNext() {
    return keyValueIndex < keyValues.size();
  }
}

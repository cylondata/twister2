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
package edu.iu.dsc.tws.checkpointmanager.state;

import java.io.IOException;

import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.local.LocalFileSystem;

public class FileStateHandle implements StreamStateHandle {

  private static final long serialVersionUID = 350284443258002355L;

  private final Path filePath;

  private final long stateSize;

  public FileStateHandle(Path filePath, long stateSize) {
    this.filePath = filePath;
    this.stateSize = stateSize;
  }


  @Override
  public FSDataInputStream openInputStream() throws IOException {
    return getFileSystem().open(filePath);
  }

  public Path getFilePath() {
    return filePath;
  }

  public void discardState() throws Exception {
    LocalFileSystem fs = getFileSystem();
    fs.delete(filePath, false);
  }


  public long getStateSize() {
    return stateSize;
  }


  //todo change this ASAP
  private LocalFileSystem getFileSystem() throws IOException {
    return new LocalFileSystem();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileStateHandle)) {
      return false;
    }

    FileStateHandle that = (FileStateHandle) o;
    return filePath.equals(that.filePath);

  }

  @Override
  public int hashCode() {
    return filePath.hashCode();
  }

  @Override
  public String toString() {
    return String.format("File State: %s [%d bytes]", filePath, stateSize);
  }
}

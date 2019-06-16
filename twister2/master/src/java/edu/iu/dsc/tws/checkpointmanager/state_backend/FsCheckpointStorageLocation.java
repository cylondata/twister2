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
package edu.iu.dsc.tws.checkpointmanager.state_backend;

import java.io.IOException;

import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

public class FsCheckpointStorageLocation extends FsCheckpointStreamFactory {

  private final FileSystem fileSystem;

  private final Path checkpointDirectory;

  private final Path sharedStateDirectory;

  private final Path taskOwnedStateDirectory;

//    private final Path metadataFilePath;

  private final int fileStateSizeThreshold;

  public FsCheckpointStorageLocation(
      FileSystem fileSystem,
      Path checkpointDir,
      Path sharedStateDir,
      Path taskOwnedStateDir,
      int fileStateSizeThreshold) {

    super(checkpointDir, sharedStateDir, fileStateSizeThreshold, fileSystem);


    this.fileSystem = fileSystem;
    this.checkpointDirectory = checkpointDir;
    this.sharedStateDirectory = sharedStateDir;
    this.taskOwnedStateDirectory = taskOwnedStateDir;


// this.metadataFilePath = new Path(checkpointDir, AbstractFsCheckpointStorage.METADATA_FILE_NAME);
    this.fileStateSizeThreshold = fileStateSizeThreshold;
  }

  // ------------------------------------------------------------------------
  //  Properties
  // ------------------------------------------------------------------------

  public Path getCheckpointDirectory() {
    return checkpointDirectory;
  }

  public Path getSharedStateDirectory() {
    return sharedStateDirectory;
  }

  public Path getTaskOwnedStateDirectory() {
    return taskOwnedStateDirectory;
  }

//    public Path getMetadataFilePath() {
//        return metadataFilePath;
//    }


  public void disposeOnFailure() throws IOException {
    // on a failure, no chunk in the checkpoint directory needs to be saved, so
    // we can drop it as a whole
    fileSystem.delete(checkpointDirectory, true);
  }


  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "FsCheckpointStorageLocation {"
        + "fileSystem=" + fileSystem
        + ", checkpointDirectory=" + checkpointDirectory
        + ", sharedStateDirectory=" + sharedStateDirectory
        + ", taskOwnedStateDirectory=" + taskOwnedStateDirectory
//        +    ", metadataFilePath=" + metadataFilePath
        + ", fileStateSizeThreshold=" + fileStateSizeThreshold
        + '}';
  }
}

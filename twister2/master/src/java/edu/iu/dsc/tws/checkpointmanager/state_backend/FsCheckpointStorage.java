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

import javax.print.attribute.standard.JobName;

import edu.iu.dsc.tws.checkpointmanager.CompletedCheckpointStorageLocation;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

public class FsCheckpointStorage extends AbstractFsCheckpointStorage {

  private final FileSystem fileSystem;

  private final Path checkpointsDirectory;

  private final Path sharedStateDirectory;


  private final int fileSizeThreshold;

  public FsCheckpointStorage(JobName jobName, FileSystem fileSystem, Path checkpointsDirectory,
                             Path sharedStateDirectory, int fileSizeThreshold) throws IOException {
    super(jobName);
    this.fileSystem = fileSystem;
    this.checkpointsDirectory = checkpointsDirectory;
    this.sharedStateDirectory = sharedStateDirectory;
    this.fileSizeThreshold = fileSizeThreshold;

    // initialize the dedicated directories
    fileSystem.mkdirs(checkpointsDirectory);
    fileSystem.mkdirs(sharedStateDirectory);

  }

  //  todo :have to finish checkpoint storage location
  public void initializeLocationForCheckpoint(long checkpointId) throws IOException {

    // create the paths needed for the checkpoints
    final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

    fileSystem.mkdirs(checkpointDir);

  }

  @Override
  public boolean supportsHighlyAvailableStorage() {
    return true;
  }

  @Override
  public boolean hasDefaultSavepointLocation() {
    return false;
  }

  @Override
  public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
      throws IOException {
    return null;
  }

}

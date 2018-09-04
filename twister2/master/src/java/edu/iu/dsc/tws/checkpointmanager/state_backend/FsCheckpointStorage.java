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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.CompletedCheckpointStorageLocation;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

public class FsCheckpointStorage extends AbstractFsCheckpointStorage {

  private static final Logger LOG = Logger.getLogger(FsCheckpointStorage.class.getName());

  private final FileSystem fileSystem;

  private final Path checkpointsDirectory;

  private final Path sharedStateDirectory;

  private final Path taskOwnedStateDirectory;

  private final int fileSizeThreshold;

  public FsCheckpointStorage(
      FileSystem fs,
      Path checkpointBaseDirectory,
      Path defaultSavepointDirectory,
      String jobName,
      int fileSizeThreshold) {

    super(jobName);


    //todo check the JobName
    this.fileSystem = fs;
    this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory,
        jobName);
    this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
    this.taskOwnedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
    this.fileSizeThreshold = fileSizeThreshold;

    // initialize the dedicated directories
    try {
      fileSystem.mkdirs(checkpointsDirectory);
      fileSystem.mkdirs(sharedStateDirectory);
      fileSystem.mkdirs(taskOwnedStateDirectory);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "checkpoint storage location is not initialized", e);
    }
  }

  public Path getCheckpointsDirectory() {
    return checkpointsDirectory;
  }

  public FsCheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
      throws IOException {

    // prepare all the paths needed for the checkpoints
    final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

    // create the checkpoint exclusive directory
    fileSystem.mkdirs(checkpointDir);

    return new FsCheckpointStorageLocation(
        fileSystem,
        checkpointDir,
        sharedStateDirectory,
        taskOwnedStateDirectory,
        fileSizeThreshold);
  }

  public FsCheckpointStreamFactory.FsCheckpointStateOutputStream createTaskOwnedStateStream()
      throws IOException {
    return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
        taskOwnedStateDirectory,
        fileSystem,
        FsCheckpointStreamFactory.DEFAULT_WRITE_BUFFER_SIZE,
        fileSizeThreshold);
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

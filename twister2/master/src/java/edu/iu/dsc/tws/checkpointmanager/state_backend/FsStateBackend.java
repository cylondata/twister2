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

import edu.iu.dsc.tws.checkpointmanager.CheckpointStorage;
import edu.iu.dsc.tws.checkpointmanager.CompletedCheckpointStorageLocation;
import edu.iu.dsc.tws.data.fs.Path;

public class FsStateBackend extends AbstractFileStateBackend {

  private static final long serialVersionUID = -8191916350224044011L;
  public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
  private final Boolean asynchronousSnapshots;

  public FsStateBackend(String checkpointDataUri, boolean asynchronousSnapshots) {
    this(new Path(checkpointDataUri), asynchronousSnapshots);
  }

  public FsStateBackend(Path baseCheckpointPath, boolean asynchronousSnapshots) {
    super(baseCheckpointPath);
    this.asynchronousSnapshots = asynchronousSnapshots;
  }

  @Override
  public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
      throws IOException {
    return null;
  }

  @Override
  public CheckpointStorage createCheckpointStorage(JobName jobname) throws IOException {
    return null;
  }

  @Override
  public AbstractKeyedStateBackend createKeyedStateBackend(
      JobName jobname, String operatorIdentifier) throws IOException {
    return null;
  }

  @Override
  public OperatorStateBackend createOperatorStateBackend(String operatorIdentifier)
      throws Exception {
    return null;
  }
}

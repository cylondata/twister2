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

public abstract class AbstractFsCheckpointStorage implements CheckpointStorage {


  public static final String CHECKPOINT_DIR_PREFIX = "chk-";

  public static final String CHECKPOINT_SHARED_STATE_DIR = "shared";

  public static final String CHECKPOINT_TASK_OWNED_STATE_DIR = "taskowned";

  public static final String METADATA_FILE_NAME = "_metadata";

  private static final byte[] REFERENCE_MAGIC_NUMBER = new byte[]{0x05, 0x5F, 0x3F, 0x18};

  private final JobName jobName;


  protected AbstractFsCheckpointStorage(JobName jobName) {
    this.jobName = jobName;
  }

  protected static CompletedCheckpointStorageLocation resolveCheckpointPointer(
      String checkpointPointer) throws IOException {
    return null;
  }

  protected static Path createCheckpointDirectory(Path baseDirectory, long checkpointId) {
    return new Path(baseDirectory, CHECKPOINT_DIR_PREFIX + checkpointId);
  }
}

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
package edu.iu.dsc.tws.checkpointmanager;

import java.util.logging.Logger;

public class CheckpointMetadata implements java.io.Serializable {
  private static final long serialVersionUID = -2387652345781312442L;

  private static final Logger LOG = Logger.getLogger(CheckpointMetadata.class.getName());

  /** The ID of the checkpoint */
  private final long checkpointId;

  /** The timestamp of the checkpoint */
  private final long timestamp;

  public CheckpointMetadata(long checkpointId, long timestamp) {
    this.checkpointId = checkpointId;
    this.timestamp = timestamp;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  public long getTimestamp() {
    return timestamp;
  }

}

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
package edu.iu.dsc.tws.checkpointmanager.messages;

import java.util.logging.Logger;

public class NotifyCheckpointComplete extends AbstractCheckpointMessage {
  private static final long serialVersionUID = 2094094662279578953L;

  private static final Logger LOG = Logger.getLogger(NotifyCheckpointComplete.class.getName());

  /** The timestamp associated with the checkpoint */
  private final long timestamp;

  public NotifyCheckpointComplete(long checkpointId, long timestamp) {
    super(checkpointId);
    this.timestamp = timestamp;
  }
}

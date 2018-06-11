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
package edu.iu.dsc.tws.checkpointmanager.barrier;

import edu.iu.dsc.tws.checkpointmanager.CheckpointOptions;

public class CheckpointBarrier extends RuntimeEvent {

  private long id;
  private long timestamp;
  private CheckpointOptions checkpointOptions;

  public CheckpointBarrier() {

  }

  public CheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) {
    this.id = id;
    this.timestamp = timestamp;
    //this.checkpointOptions = checkNotNull(checkpointOptions);
    this.checkpointOptions = checkpointOptions;
  }

  public long getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public CheckpointOptions getCheckpointOptions() {
    return checkpointOptions;
  }

  public int hashCode() {
    return (int) (id ^ (id >>> 32) ^ timestamp ^ (timestamp >>> 32));
  }

  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other == null || other.getClass() != CheckpointBarrier.class) {
      return false;
    } else {
      CheckpointBarrier that = (CheckpointBarrier) other;
      return that.id == this.id && that.timestamp == this.timestamp
          && this.checkpointOptions.equals(that.checkpointOptions);
    }
  }

  public String toString() {
    return String.format("CheckpointBarrier %d @ %d Options: %s", id, timestamp, checkpointOptions);
  }
}


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
package edu.iu.dsc.tws.common.zk;

import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;

public class WorkerWithState {
  public static final Logger LOG = Logger.getLogger(WorkerWithState.class.getName());

  private WorkerInfo info;
  private WorkerState state;

  public WorkerWithState(WorkerInfo info, WorkerState state) {
    this.info = info;
    this.state = state;
  }

  public WorkerInfo getInfo() {
    return info;
  }

  public WorkerState getState() {
    return state;
  }

  public int getWorkerID() {
    return info.getWorkerID();
  }

  public void setState(WorkerState state) {
    this.state = state;
  }

  public byte[] toByteArray() {
    byte[] stateNameBytes;
    try {
      stateNameBytes = state.name().getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      LOG.log(Level.WARNING, "UTF8 Unsupported. Using default encoding instead.");
      stateNameBytes = state.name().getBytes();
    }
    byte[] stateLengthBytes = Ints.toByteArray(stateNameBytes.length);
    byte[] workerInfoBytes = info.toByteArray();

    return Bytes.concat(stateLengthBytes, stateNameBytes, workerInfoBytes);
  }

  public static WorkerWithState decode(byte[] encodedBytes) {
    if (encodedBytes == null) {
      return null;
    }

    // first 4 bytes is the length of worker state
    int stateLength = Ints.fromByteArray(encodedBytes);
    String stateName = new String(encodedBytes, 4, stateLength);
    WorkerState workerState = WorkerState.valueOf(stateName);

    try {
      WorkerInfo workerInfo = WorkerInfo.newBuilder()
          .mergeFrom(encodedBytes, 4 + stateLength, encodedBytes.length - 4 - stateLength)
          .build();
      return new WorkerWithState(workerInfo, workerState);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Could not decode received byte array as a WorkerInfo object", e);
      return null;
    }
  }

  /**
   * return true if the worker status is either or STARTED, RESTARTED
   * @return
   */
  public boolean running() {
    return state == WorkerState.STARTED
        || state == WorkerState.RESTARTED;
  }

  /**
   * return true if the worker status is either or STARTED, RESTARTED, COMPLETED
   * @return
   */
  public boolean startedOrCompleted() {
    return state == WorkerState.STARTED
        || state == WorkerState.RESTARTED
        || state == WorkerState.COMPLETED;
  }

  /**
   * if the worker state is COMPLETED
   * @return
   */
  public boolean completed() {
    return state == WorkerState.COMPLETED;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkerWithState that = (WorkerWithState) o;
    return info.getWorkerID() == that.info.getWorkerID();
  }

  @Override
  public int hashCode() {
    return Objects.hash(info.getWorkerID());
  }

}

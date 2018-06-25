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
package edu.iu.dsc.tws.task.graph;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TaskCheckpointingSettings {
  private static final long serialVersionUID = -2593319571078198180L;

  private final List<Vertex> verticesToTrigger;

  private final List<Vertex> verticesToAcknowledge;

  private final List<Vertex> verticesToConfirm;

  private final CheckpointManagerConfiguration checkpointManagerConfiguration;

  //private final SerializedValue<StateBackend> defaultStateBackend;

  //private final SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks;

  public TaskCheckpointingSettings(
      List<Vertex> verticesToTrigger,
      List<Vertex> verticesToAcknowledge,
      List<Vertex> verticesToConfirm,
      CheckpointManagerConfiguration checkpointManagerConfiguration) {


    this.verticesToTrigger = requireNonNull(verticesToTrigger);
    this.verticesToAcknowledge = requireNonNull(verticesToAcknowledge);
    this.verticesToConfirm = requireNonNull(verticesToConfirm);
    this.checkpointManagerConfiguration = checkpointManagerConfiguration;

  }

  // --------------------------------------------------------------------------------------------

  public List<Vertex> getVerticesToTrigger() {
    return verticesToTrigger;
  }

  public List<Vertex> getVerticesToAcknowledge() {
    return verticesToAcknowledge;
  }

  public List<Vertex> getVerticesToConfirm() {
    return verticesToConfirm;
  }

  public CheckpointManagerConfiguration getCheckpointManagerConfiguration() {
    return checkpointManagerConfiguration;
  }
  // --------------------------------------------------------------------------------------------

  @Override
  public String toString() {
    return String.format("SnapshotSettings: config=%s, trigger=%s, ack=%s, commit=%s",
        checkpointManagerConfiguration,
        verticesToTrigger,
        verticesToAcknowledge,
        verticesToConfirm);
  }
}

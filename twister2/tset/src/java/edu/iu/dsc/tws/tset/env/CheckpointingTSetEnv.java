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
package edu.iu.dsc.tws.tset.env;

import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.checkpointing.worker.CheckpointingWorkerEnv;

public class CheckpointingTSetEnv extends BatchTSetEnvironment {

  private CheckpointingWorkerEnv workerEnvironment;

  public CheckpointingTSetEnv(WorkerEnvironment workerEnvironment) {
    super(workerEnvironment);
    this.workerEnvironment = CheckpointingWorkerEnv.newBuilder(workerEnvironment).build();
  }

  /**
   * Updates the variable in the snapshot
   */
  public <T> T updateVariable(String name, T value) {
    this.workerEnvironment.getSnapshot().setValue(name, value);
    return value;
  }

  /**
   * Initialize a variable from previous snapshot. Use default value if this
   * variable is not defined in previous snapshot
   */
  public <T> T initVariable(String name, T defaultValue) {
    Object value = this.workerEnvironment.getSnapshot().get(name);
    if (value == null) {
      return this.updateVariable(name, defaultValue);
    }
    return (T) value;
  }

  public void commit() {
    this.workerEnvironment.commitSnapshot();
  }
}

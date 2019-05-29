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
package edu.iu.dsc.tws.examples.checkpointing;

import edu.iu.dsc.tws.api.task.SourceConnection;
import edu.iu.dsc.tws.api.task.TaskEnvironment;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.ftolerance.api.Snapshot;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.checkpoint.Checkpointable;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class CheckpointingTaskExample implements IWorker {

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    TaskEnvironment taskEnvironment = TaskEnvironment.init(
        config, workerID, workerController, volatileVolume);

    TaskGraphBuilder taskGraphBuilder = taskEnvironment.newTaskGraph(OperationMode.STREAMING);

    SourceConnection source = taskGraphBuilder.addSource("source", new SourceTask());
    taskGraphBuilder.addSink("sink", null);
  }


  public static class SourceTask extends BaseSource implements Checkpointable {

    private int count = 0;

    @Override
    public void execute() {
      context.write("edge", count++);
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      this.count = (int) snapshot.getOrDefault("count", 0);

    }

    @Override
    public void takeSnapshot(Snapshot snapshot) {
      snapshot.setValue("count", this.count);
    }

    @Override
    public void initSnapshot(Snapshot snapshot) {
      snapshot.setPacker("count", MessageTypes.INTEGER.getDataPacker());
    }
  }

  public static void main(String[] args) {

  }
}

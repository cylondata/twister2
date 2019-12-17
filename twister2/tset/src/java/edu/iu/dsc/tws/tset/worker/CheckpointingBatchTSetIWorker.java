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
package edu.iu.dsc.tws.tset.worker;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.master.worker.JMSenderToDriver;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.system.JobExecutionState;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;

public interface CheckpointingBatchTSetIWorker extends IWorker {

  @Override
  default void execute(Config config, int workerID, IWorkerController workerController,
                       IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    WorkerEnvironment workerEnv = WorkerEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    JMSenderToDriver senderToDriver = JMWorkerAgent.getJMWorkerAgent().getSenderToDriver();

    this.execute(new CheckpointingTSetEnv(workerEnv));

    //If the execute returns without any errors we assume that the job completed properly
    JobExecutionState.WorkerJobState workerState =
        JobExecutionState.WorkerJobState.newBuilder()
            .setFailure(false)
            .setJobName(config.getStringValue(Context.JOB_NAME))
            .setWorkerMessage("Worker Completed")
            .build();
    senderToDriver.sendToDriver(workerState);
  }

  void execute(CheckpointingTSetEnv env);
}

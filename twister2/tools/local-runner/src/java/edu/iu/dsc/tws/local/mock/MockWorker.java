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
package edu.iu.dsc.tws.local.mock;

import java.util.concurrent.CyclicBarrier;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class MockWorker implements Runnable {

  private Config config;
  private int workerId;
  private Twister2Worker twister2Worker;
  private IWorker iworker; // keeping backward compatibility
  private MockWorkerController mockWorkerController;
  private JobAPI.Job job;

  public MockWorker(Twister2Job twister2Job,
                    Config config,
                    Integer workerId,
                    CyclicBarrier cyclicBarrier) {
    this.config = config;
    this.workerId = workerId;
    this.job = twister2Job.serialize();

    try {
      Object worker = this.getClass().getClassLoader()
          .loadClass(twister2Job.getWorkerClass()).newInstance();

      if (worker instanceof Twister2Worker) {
        this.twister2Worker = (Twister2Worker) worker;
      } else if (worker instanceof IWorker) {
        this.iworker = (IWorker) worker;
      } else {
        throw new Twister2RuntimeException("Unsupported Worker class type " + worker.getClass());
      }
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new Twister2RuntimeException("Error in creating worker instance", e);
    }

    this.mockWorkerController = new MockWorkerController(twister2Job, config,
        workerId, cyclicBarrier);
  }

  @Override
  public void run() {
    if (this.twister2Worker != null) {
      WorkerEnvironment workerEnvironment = WorkerEnvironment.init(this.config, this.job,
          this.mockWorkerController,
          null, null);
      this.twister2Worker.execute(workerEnvironment);
    } else {
      this.iworker.execute(this.config, this.job, this.mockWorkerController, null, null);
    }
  }


}

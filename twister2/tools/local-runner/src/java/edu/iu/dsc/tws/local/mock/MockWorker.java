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

public class MockWorker implements Runnable {

  private Config config;
  private int workerId;
  private IWorker iWorker;
  private MockWorkerController mockWorkerController;

  public MockWorker(Twister2Job twister2Job,
                    Config config,
                    Integer workerId,
                    CyclicBarrier cyclicBarrier) {
    this.config = config;
    this.workerId = workerId;
    try {
      this.iWorker = (IWorker) this.getClass().getClassLoader()
          .loadClass(twister2Job.getWorkerClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new Twister2RuntimeException("Error in creating worker instance", e);
    }

    this.mockWorkerController = new MockWorkerController(twister2Job, config,
        workerId, cyclicBarrier);
  }

  @Override
  public void run() {
    this.iWorker.execute(
        this.config,
        this.workerId,
        this.mockWorkerController,
        null,
        null
    );
  }


}

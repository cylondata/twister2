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
package edu.iu.dsc.tws.api.resource;

import edu.iu.dsc.tws.api.config.Config;

public class WorkerControl {
  private Config config;
  private int workerID;
  private IWorkerController workerController;
  private IPersistentVolume persistentVolume;
  private IVolatileVolume volatileVolume;

  public WorkerControl(Config config, int workerID, IWorkerController workerController,
                       IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.config = config;
    this.workerID = workerID;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;
  }

  public Config getConfig() {
    return config;
  }

  public int getWorkerID() {
    return workerID;
  }

  public IWorkerController getWorkerController() {
    return workerController;
  }

  public IPersistentVolume getPersistentVolume() {
    return persistentVolume;
  }

  public IVolatileVolume getVolatileVolume() {
    return volatileVolume;
  }
}

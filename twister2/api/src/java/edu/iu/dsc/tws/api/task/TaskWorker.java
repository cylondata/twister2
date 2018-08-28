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
package edu.iu.dsc.tws.api.task;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;

public class TaskWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(TaskWorker.class.getName());

  protected TWSChannel channel;

  protected Communicator communicator;

  @Override
  public void init(Config config, int workerID, AllocatedResources allocatedResources,
                   IWorkerController workerController, IPersistentVolume persistentVolume,
                   IVolatileVolume volatileVolume) {
    // create the channel
    channel = Network.initializeChannel(config, workerController, allocatedResources);
    // create the communicator
    communicator = new Communicator(config, channel);
  }
}

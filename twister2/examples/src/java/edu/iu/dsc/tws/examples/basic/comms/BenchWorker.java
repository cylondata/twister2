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
package edu.iu.dsc.tws.examples.basic.comms;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerDiscoverer;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.spi.container.IPersistentVolume;
import edu.iu.dsc.tws.rsched.spi.container.IVolatileVolume;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public abstract class BenchWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(BenchWorker.class.getName());

  protected ResourcePlan resourcePlan;

  protected int id;

  protected Config config;

  protected TaskPlan taskPlan;

  protected JobParameters jobParameters;

  protected TWSChannel channel;

  protected Communicator communicator;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan,
                   IWorkerDiscoverer workerController, IPersistentVolume persistentVolume,
                   IVolatileVolume volatileVolume) {
    // create the job parameters
    this.jobParameters = JobParameters.build(cfg);
    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;

    // lets create the task plan
    this.taskPlan = Utils.createStageTaskPlan(cfg, plan, jobParameters.getTaskStages());
    // create the channel
    channel = Network.initializeChannel(config, workerController, resourcePlan);
    // create the communicator
    communicator = new Communicator(cfg, channel);
    // now lets execute
    execute();
    // now progress
    progress();
  }

  protected abstract void execute();

  protected void progress() {
    // we need to progress the communication
    while (!isDone()) {
      // progress the channel
      channel.progress();
      // we should progress the communication directive
      progressCommunication();
    }
  }

  protected abstract void progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object data, int flag);

  protected class MapWorker implements Runnable {
    private int task;

    public MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker: " + id);
      int[] data = DataGenerator.generateIntData(jobParameters.getSize());
      for (int i = 0; i < jobParameters.getIterations(); i++) {
        // lets generate a message
        int flag = 0;
        if (i == jobParameters.getIterations() - 1) {
          flag = MessageFlags.FLAGS_LAST;
        }
        sendMessages(task, data, flag);
      }
      LOG.info(String.format("%d Done sending", id));
    }
  }
}

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
package edu.iu.dsc.tws.examples.streaming.wordcount.comms;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.Network;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.stream.SPartition;
import edu.iu.dsc.tws.examples.utils.WordCountUtils;

public class WordCountWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(WordCountWorker.class.getName());

  private SPartition keyedPartition;

  private Communicator channel;

  private static final int NO_OF_TASKS = 8;

  private Config config;

  private int id;

  private int noOfTasksPerExecutor;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private LogicalPlan logicalPlan;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.config = cfg;
    this.id = workerID;
    this.noOfTasksPerExecutor = NO_OF_TASKS / workerController.getNumberOfWorkers();

    setupTasks(workerController);
    setupNetwork(workerController);

    // create the communication
    keyedPartition = new SPartition(channel, logicalPlan, sources, destinations,
        MessageTypes.OBJECT, new WordAggregate(), new HashingSelector());

    scheduleTasks();
    progress();
  }

  private void setupTasks(IWorkerController workerController) {
    try {
      logicalPlan = WordCountUtils.createWordCountPlan(config, id,
          workerController.getAllWorkers(), NO_OF_TASKS);
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      sources.add(i);
    }
    destinations = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      destinations.add(NO_OF_TASKS / 2 + i);
    }
    LOG.fine(String.format("%d sources %s destinations %s",
        logicalPlan.getThisWorker(), sources, destinations));
  }

  private void setupNetwork(IWorkerController controller) {
    TWSChannel twsChannel = Network.initializeChannel(config, controller);
    this.channel = new Communicator(config, twsChannel);
  }

  private void scheduleTasks() {
    if (id < 2) {
      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
        Thread mapThread = new Thread(new StreamingWordSource(keyedPartition, 1000,
            noOfTasksPerExecutor * id + i, 10));
        mapThread.start();
      }
    }
  }

  private void progress() {
    // we need to communicationProgress the communication
    while (true) {
      try {
        // communicationProgress the channel
        channel.getChannel().progress();
        // we should communicationProgress the communication directive
        keyedPartition.progress();
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, "Error", t);
      }
    }
  }
}

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
package edu.iu.dsc.tws.api.task.harp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.harp.client.SyncClient;
import edu.iu.harp.collective.Communication;
import edu.iu.harp.io.Constant;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.server.Server;
import edu.iu.harp.worker.Workers;

/**
 * This class starts Harp Servers inside twiser2 worker and allow user to use Harp APIs
 * inside logic execution
 */
public abstract class HarpWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(HarpWorker.class.getName());

  @Override
  public void execute(Config config, int workerID, AllocatedResources allocatedResources,
                      IWorkerController workerController, IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    WorkerNetworkInfo workerNetworkInfo = workerController.getWorkerNetworkInfo();

    //Building Harp Specific parameters
    Map<String, Integer> rackToIntegerMap = this.getRackToIntegerMap(workerController);
    LinkedList<Integer> nodeRackIDs = new LinkedList<>(rackToIntegerMap.values());
    int noOfPhysicalNodes = nodeRackIDs.size(); //todo check the suitability
    Map<Integer, List<String>> nodesOfRackMap = this.getNodesOfRackMap(workerController);

    Workers workers = new Workers(nodesOfRackMap, nodeRackIDs, noOfPhysicalNodes, workerID);
    DataMap dataMap = new DataMap();

    int harpPort = Constant.DEFAULT_WORKER_POART_BASE + workerID;

    Server server;
    try {
      server = new Server(
          workerNetworkInfo.getWorkerIP().getHostAddress(),
          harpPort,
          new EventQueue(),
          dataMap,
          workers
      );
    } catch (Exception e) {
      LOG.log(Level.SEVERE, String.format("Failed to start harp server %s:%d "
              + "on twister worker %s:%d",
          workerNetworkInfo.getWorkerIP().getHostAddress(),
          harpPort,
          workerNetworkInfo.getWorkerIP().getHostAddress(),
          workerNetworkInfo.getWorkerPort()),
          e);
      throw new RuntimeException("Failed to start Harp Server");
    }

    SyncClient syncClient = new SyncClient(workers);
    LOG.info(String.format("Starting harp server on port : %d", harpPort));
    server.start();

    LOG.info("Starting Harp Sync client");
    syncClient.start();

    try {
      LOG.info("Trying master barrier");
      doMasterBarrier("start-worker", "handshake", dataMap, workers);
      LOG.info("Master barrier done");
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to do master barrier", e);
      server.stop();
      syncClient.stop();
      throw new RuntimeException("Failed to do master barrier");
    }

    //call executeHarp that will be coded by user
    this.executeHarp(config, workerID, allocatedResources, workerController, persistentVolume,
        volatileVolume, dataMap, workers);

    //stopping servers, releasing resources
    LOG.info("Execution completed. Shutting harp server down...");
    server.stop();
    syncClient.stop();
  }

  private void doMasterBarrier(String contextName, String operationName,
                               DataMap dataMap, Workers workers) throws IOException {
    boolean successful = Communication.barrier(contextName, operationName, dataMap, workers);
    dataMap.cleanOperationData(contextName, operationName);
    if (!successful) {
      throw new IOException("Failed to do master barrier");
    }
  }

  private Map<Integer, List<String>> getNodesOfRackMap(IWorkerController workerController) {
    Map<String, Integer> racks = this.getRackToIntegerMap(workerController);

    Map<Integer, List<String>> nodesOfRack = new HashMap<>();

    workerController.getWorkerList().forEach(worker -> {
      Integer rackKey = racks.get(getRackKey(worker.getNodeInfo()));
      nodesOfRack.computeIfAbsent(rackKey, ArrayList::new);
      nodesOfRack.get(rackKey).add(worker.getWorkerIP().getHostAddress());
    });

    return nodesOfRack;
  }

  /**
   * Harp expects rack ID to be a numeric value while Twister2 allows alphanumeric.
   * This method create a mapping from possibly alphanumeric rack id to numeric rack id that
   * can be fed to Harp
   *
   * @param workerController instance of Twister2 {@link IWorkerController}
   * @return Alphanumeric to numeric mapping of rack IDs
   */
  private Map<String, Integer> getRackToIntegerMap(IWorkerController workerController) {
    List<WorkerNetworkInfo> workerList = workerController.getWorkerList();
    AtomicInteger counter = new AtomicInteger();
    return workerList.stream()
        .map(WorkerNetworkInfo::getNodeInfo)
        .map(this::getRackKey)
        .distinct()
        .sorted()
        .collect(Collectors.toMap(k -> k, v -> counter.getAndDecrement()));
  }

  /**
   * Returns an unique ID for a rack
   *
   * @return unique id for the rack
   */
  private String getRackKey(NodeInfo nodeInfo) {
    return nodeInfo.hasDataCenterName() ? //multiple data centers can have same rack name
        nodeInfo.getDataCenterName().concat(nodeInfo.getRackName()) : nodeInfo.getRackName();
  }

  /**
   * This method will be called after setting up Harp environment
   */
  public abstract void executeHarp(Config config, int workerID,
                                   AllocatedResources allocatedResources,
                                   IWorkerController workerController,
                                   IPersistentVolume persistentVolume,
                                   IVolatileVolume volatileVolume,
                                   DataMap harpDataMap, Workers harpWorkers);
}

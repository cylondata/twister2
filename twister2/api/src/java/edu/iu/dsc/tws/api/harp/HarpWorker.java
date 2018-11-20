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
package edu.iu.dsc.tws.api.harp;

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
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
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
  public void execute(Config config, int workerID,
                      IWorkerController workerController, IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    List<JobMasterAPI.WorkerInfo> workersList = workerController.getAllWorkers();

    LOG.info(String.format("Worker %s starting with %d workers, "
            + "after waiting for all to start. \n %s",
        workerID, workersList.size(), workersList.toString()));

    JobMasterAPI.WorkerInfo workerInfo = workerController.getWorkerInfo();

    //Building Harp Specific parameters
    Map<String, Integer> rackToIntegerMap = this.getRackToIntegerMap(workersList);
    LinkedList<Integer> nodeRackIDs = new LinkedList<>(rackToIntegerMap.values());
    int noOfPhysicalNodes = nodeRackIDs.size(); //todo check the suitability
    Map<Integer, List<String>> nodesOfRackMap = this.getNodesOfRackMap(workersList,
        rackToIntegerMap);

    Workers workers = new Workers(nodesOfRackMap, nodeRackIDs, noOfPhysicalNodes, workerID);
    DataMap dataMap = new DataMap();

    int harpPort = Constant.DEFAULT_WORKER_POART_BASE + workerID;

    Server server;
    try {
      server = new Server(
          workerInfo.getWorkerIP(),
          harpPort,
          new EventQueue(),
          dataMap,
          workers
      );
    } catch (Exception e) {
      LOG.log(Level.SEVERE, String.format("Failed to start harp server %s:%d "
              + "on twister worker %s:%d",
          workerInfo.getWorkerIP(),
          harpPort,
          workerInfo.getWorkerIP(),
          workerInfo.getPort()),
          e);
      throw new RuntimeException("Failed to start Harp Server");
    }

    SyncClient syncClient = new SyncClient(workers);
    LOG.info("Starting Harp Sync client");
    syncClient.start();

    LOG.info(String.format("Starting harp server on port : %d", harpPort));
    server.start();
    LOG.info(String.format("Harp server started. %s:%d "
            + "on twister worker %s:%d",
        workerInfo.getWorkerIP(),
        harpPort,
        workerInfo.getWorkerIP(),
        workerInfo.getPort()));

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
    this.executeHarp(config, workerID, workerController.getNumberOfWorkers(), workerController,
        persistentVolume, volatileVolume, dataMap, workers);

    //stopping servers, releasing resources
    LOG.info("Execution completed. Shutting harp Sync Client down....");
    syncClient.stop();
    LOG.info("Harp Sync Client stopped.");
    LOG.info("Shutting harp server down....");
    server.stop(true);
    LOG.info("Harp server stopped.");
  }

  private void doMasterBarrier(String contextName, String operationName,
                               DataMap dataMap, Workers workers) throws IOException {
    boolean successful = Communication.barrier(contextName, operationName, dataMap, workers);
    dataMap.cleanOperationData(contextName, operationName);
    if (!successful) {
      throw new IOException("Failed to do master barrier");
    }
  }

  private Map<Integer, List<String>> getNodesOfRackMap(List<JobMasterAPI.WorkerInfo> workerList,
                                                       Map<String, Integer> racks) {
    Map<Integer, List<String>> nodesOfRack = new HashMap<>();

    workerList.forEach(worker -> {
      Integer rackKey = racks.get(getRackKey(worker.getNodeInfo()));
      nodesOfRack.computeIfAbsent(rackKey, integer -> new ArrayList<>());
      nodesOfRack.get(rackKey).add(worker.getWorkerIP());
    });

    return nodesOfRack;
  }

  /**
   * Harp expects rack ID to be a numeric value while Twister2 allows alphanumeric.
   * This method create a mapping from possibly alphanumeric rack id to numeric rack id that
   * can be fed to Harp
   *
   * @return Alphanumeric to numeric mapping of rack IDs
   */
  private Map<String, Integer> getRackToIntegerMap(List<JobMasterAPI.WorkerInfo> workerList) {
    AtomicInteger counter = new AtomicInteger();
    return workerList.stream()
        .map(JobMasterAPI.WorkerInfo::getNodeInfo)
        .map(this::getRackKey)
        .distinct()
        .sorted()
        .collect(Collectors.toMap(k -> k, v -> counter.getAndIncrement()));
  }

  /**
   * Returns an unique ID for a rack
   *
   * @return unique id for the rack
   */
  private String getRackKey(JobMasterAPI.NodeInfo nodeInfo) {
    return nodeInfo.getDataCenterName().isEmpty() ? //multiple data centers can have same rack name
        nodeInfo.getDataCenterName().concat(nodeInfo.getRackName()) : nodeInfo.getRackName();
  }

  /**
   * This method will be called after setting up Harp environment
   */
  public abstract void executeHarp(Config config, int workerID,
                                   int numberOfWorkers,
                                   IWorkerController workerController,
                                   IPersistentVolume persistentVolume,
                                   IVolatileVolume volatileVolume,
                                   DataMap harpDataMap, Workers harpWorkers);
}

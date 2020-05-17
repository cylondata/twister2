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
package edu.iu.dsc.tws.checkpointing.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.faulttolerance.JobFaultListener;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class CheckpointManager implements MessageHandler, JobFaultListener {

  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());
  private static final String STR_UNDERSCORE = "_";

  private final HashMap<String, Map<Integer, CheckpointStatus>> statusMap = new HashMap<>();
  private final HashMap<String, Long> familyVersionMap = new HashMap<>();
  private final HashMap<String, FamilyInitHandler> familyInitHandlers = new HashMap<>();
  private final RRServer rrServer;
  private final StateStore stateStore;
  private final String jobId;

  public CheckpointManager(RRServer rrServer,
                           StateStore stateStore, String jobId) {
    this.rrServer = rrServer;
    this.stateStore = stateStore;
    this.jobId = jobId;
  }

  public void init() {
    this.rrServer.registerRequestHandler(Checkpoint.VersionUpdateRequest.newBuilder(), this);
    this.rrServer.registerRequestHandler(Checkpoint.ComponentDiscovery.newBuilder(), this);
    this.rrServer.registerRequestHandler(Checkpoint.FamilyInitialize.newBuilder(), this);
  }


  private void handleDiscovery(RequestID requestID, Checkpoint.ComponentDiscovery discoveryMsg) {
    Map<Integer, CheckpointStatus> familyMap = this.statusMap.computeIfAbsent(
        discoveryMsg.getFamily(), family -> new HashMap<>()
    );

    familyMap.computeIfAbsent(discoveryMsg.getIndex(),
        index -> new CheckpointStatus(discoveryMsg.getFamily(), discoveryMsg.getIndex()));

    long latestVersion = this.familyVersionMap.computeIfAbsent(
        discoveryMsg.getFamily(), family -> {
          long oldVersion = 0L;
          try {
            byte[] bytes = stateStore.get(getStateKey(family));
            if (bytes != null) {
              oldVersion = ByteBuffer.wrap(bytes).getLong();
              LOG.info(discoveryMsg.getFamily() + " will be restored to " + oldVersion);
            }
          } catch (IOException e) {
            LOG.severe(() -> "Failed to lookup older version for " + discoveryMsg.getFamily());
          }
          return oldVersion;
        });

    this.rrServer.sendResponse(
        requestID,
        Checkpoint.ComponentDiscoveryResponse.newBuilder()
            .setFamily(discoveryMsg.getFamily())
            .setIndex(discoveryMsg.getIndex())
            .setVersion(latestVersion)
            .build()
    );

    LOG.info("discovery done: status \n" + this.statusMap);
  }

  private String getStateKey(String family) {
    return String.join(STR_UNDERSCORE, this.jobId, family);
  }

  private void handleVersionUpdate(RequestID requestID,
                                   Checkpoint.VersionUpdateRequest versionUpdateMsg) {
    LOG.fine(() -> "Version update request received from : "
        + versionUpdateMsg.getFamily() + " : " + versionUpdateMsg.getIndex() + " with version "
        + versionUpdateMsg.getVersion());
    Map<Integer, CheckpointStatus> familyMap =
        this.statusMap.get(versionUpdateMsg.getFamily());
    if (familyMap == null) {
      LOG.severe(() -> "Received a version update message from an unknown family, "
          + versionUpdateMsg.getFamily());
      return;
    }

    CheckpointStatus checkpointStatus = familyMap.get(versionUpdateMsg.getIndex());
    if (checkpointStatus == null) {
      LOG.severe(() -> "Received a version update message from an unknown index "
          + versionUpdateMsg.getIndex() + " of family " + versionUpdateMsg.getFamily());
      return;
    }

    checkpointStatus.setVersion(versionUpdateMsg.getVersion());

    long currentVersion = this.familyVersionMap.getOrDefault(
        versionUpdateMsg.getFamily(), 0L);

    long minVersion = Long.MAX_VALUE;

    for (CheckpointStatus value : familyMap.values()) {
      if (minVersion > value.getVersion()) {
        minVersion = value.getVersion();
      }
    }

    //if version has updated
    if (minVersion > currentVersion) {
      try {
        LOG.fine("Updating the version of " + versionUpdateMsg.getFamily()
            + " to " + minVersion + " from " + currentVersion);
        this.stateStore.put(
            this.getStateKey(versionUpdateMsg.getFamily()),
            ByteBuffer.allocate(Long.BYTES).putLong(minVersion).array()
        );
        //update in memory cache
        this.familyVersionMap.put(versionUpdateMsg.getFamily(), minVersion);
      } catch (IOException e) {
        LOG.severe(() -> "Failed to persist the version of " + versionUpdateMsg.getFamily());
      }
    }

    this.rrServer.sendResponse(
        requestID,
        Checkpoint.VersionUpdateResponse.newBuilder()
            .setFamily(versionUpdateMsg.getFamily())
            .setIndex(versionUpdateMsg.getIndex())
            .build()
    );
    LOG.info("version update done: status \n" + this.statusMap);
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof Checkpoint.ComponentDiscovery) {
      this.handleDiscovery(id, (Checkpoint.ComponentDiscovery) message);
    } else if (message instanceof Checkpoint.VersionUpdateRequest) {
      this.handleVersionUpdate(id, (Checkpoint.VersionUpdateRequest) message);
    } else if (message instanceof Checkpoint.FamilyInitialize) {
      this.handleFamilyInit(id, (Checkpoint.FamilyInitialize) message);
    }
  }

  private Long getFamilyVersion(String family) {
    if (this.familyVersionMap.containsKey(family)) {
      return this.familyVersionMap.get(family);
    } else {
      long oldVersion = 0L;
      try {
        byte[] bytes = stateStore.get(getStateKey(family));
        if (bytes != null) {
          oldVersion = ByteBuffer.wrap(bytes).getLong();
          LOG.info(family + " will be restored to " + oldVersion);
        }
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to lookup older version for " + family, e);
      }
      return oldVersion;
    }
  }

  private synchronized void handleFamilyInit(RequestID id, Checkpoint.FamilyInitialize message) {
    LOG.fine("Family init request received from " + message.getContainerIndex()
        + ". Family : " + message.getFamily());

    FamilyInitHandler familyInitHandler = this.familyInitHandlers.get(message.getFamily());

    if (familyInitHandler == null) {
      Long familyVersion = this.getFamilyVersion(message.getFamily());
      familyInitHandler = new FamilyInitHandler(rrServer, message.getFamily(),
          message.getContainers(), familyVersion);
      this.familyInitHandlers.put(message.getFamily(), familyInitHandler);
    }

    // if request is coming from worker 0
    if (message.getContainerIndex() == 0) {
      List<Integer> membersList = message.getMembersList();

      Map<Integer, CheckpointStatus> checkPointsByIndex =
          this.statusMap.computeIfAbsent(message.getFamily(), f -> new HashMap<>());

      for (Integer index : membersList) {
        CheckpointStatus state = new CheckpointStatus(message.getFamily(), index);
        state.setVersion(familyInitHandler.getVersion());
        checkPointsByIndex.put(index, state);
      }
    }

    boolean sentResponses = familyInitHandler.scheduleResponse(message.getContainerIndex(), id);
    if (sentResponses) {
      LOG.info("Family " + message.getFamily() + " will start with version "
          + familyInitHandler.getVersion());
    } else {
      LOG.fine("Scheduled family init response for family : " + message.getFamily()
          + " for worker id " + message.getContainerIndex());
    }

    LOG.info("family init done: status\n" + this.statusMap);
  }

  @Override
  public synchronized void faultOccurred() {
    for (FamilyInitHandler familyInitHandler : this.familyInitHandlers.values()) {
      familyInitHandler.pause();
    }
  }

  @Override
  public synchronized void faultRestored() {
    for (FamilyInitHandler familyInitHandler : this.familyInitHandlers.values()) {
      familyInitHandler.resume();
    }
  }
}

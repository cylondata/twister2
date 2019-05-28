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
package edu.iu.dsc.tws.ftolerance.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.ftolerance.api.StateStore;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class CheckpointManager implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());
  private static final String STR_UNDERSCORE = "_";

  private final HashMap<String, Map<Integer, CheckpointStatus>> statusMap = new HashMap<>();
  private final HashMap<String, Long> familyVersionMap = new HashMap<>();
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
  }

  private String getStateKey(String family) {
    return String.join(STR_UNDERSCORE, this.jobId, family);
  }

  private void handleVersionUpdate(RequestID requestID,
                                   Checkpoint.VersionUpdateRequest versionUpdateMsg) {
    LOG.info(() -> "Version update request received from : "
        + versionUpdateMsg.getFamily() + ":" + versionUpdateMsg.getIndex() + " to "
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
        LOG.info("Updating the version of " + versionUpdateMsg.getFamily()
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
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof Checkpoint.ComponentDiscovery) {
      this.handleDiscovery(id, (Checkpoint.ComponentDiscovery) message);
    } else if (message instanceof Checkpoint.VersionUpdateRequest) {
      this.handleVersionUpdate(id, (Checkpoint.VersionUpdateRequest) message);
    }
  }
}

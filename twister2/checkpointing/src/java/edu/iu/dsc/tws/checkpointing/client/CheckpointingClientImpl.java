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

package edu.iu.dsc.tws.checkpointing.client;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.exceptions.JobFaultyException;
import edu.iu.dsc.tws.api.exceptions.net.BlockingSendException;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

/**
 * This client can be used to communicate with
 * {@link edu.iu.dsc.tws.checkpointing.master.CheckpointManager}
 */
public final class CheckpointingClientImpl implements MessageHandler, CheckpointingClient {

  private static final Logger LOG = Logger.getLogger(CheckpointingClientImpl.class.getName());

  private RRClient rrClient;
  private long waitTime;
  private Map<RequestID, Message> blockingResponse = new ConcurrentHashMap<>();
  private Map<RequestID, MessageHandler> asyncHandlers = new ConcurrentHashMap<>();

  public CheckpointingClientImpl(RRClient rrClient, long waitTime) {
    this.rrClient = rrClient;
    this.waitTime = waitTime;
  }

  public void init() {
    this.rrClient.registerMessage(Checkpoint.ComponentDiscovery.newBuilder());
    this.rrClient.registerResponseHandler(
        Checkpoint.ComponentDiscoveryResponse.newBuilder(), this);

    this.rrClient.registerMessage(Checkpoint.VersionUpdateRequest.newBuilder());
    this.rrClient.registerResponseHandler(
        Checkpoint.VersionUpdateResponse.newBuilder(), this);

    this.rrClient.registerMessage(Checkpoint.FamilyInitialize.newBuilder());
    this.rrClient.registerResponseHandler(
        Checkpoint.FamilyInitializeResponse.newBuilder(), this);
  }

  @Override
  public Checkpoint.ComponentDiscoveryResponse sendDiscoveryMessage(
      String family, int index) throws BlockingSendException {

    Tuple<RequestID, Message> response = this.rrClient.sendRequestWaitResponse(
        Checkpoint.ComponentDiscovery.newBuilder()
            .setFamily(family)
            .setIndex(index)
            .build(),
        this.waitTime
    );
    return (Checkpoint.ComponentDiscoveryResponse) this.blockingResponse.remove(response.getKey());
  }

  @Override
  public Checkpoint.FamilyInitializeResponse initFamily(int containerIndex,
                                                        int containersCount,
                                                        String family,
                                                        Set<Integer> members)
      throws BlockingSendException {

    Tuple<RequestID, Message> response = this.rrClient.sendRequestWaitResponse(
        Checkpoint.FamilyInitialize.newBuilder()
            .setFamily(family)
            .addAllMembers(members)
            .setContainerIndex(containerIndex)
            .setContainers(containersCount)
            .build(),
        this.waitTime
    );
    Checkpoint.FamilyInitializeResponse initReso =
        (Checkpoint.FamilyInitializeResponse) this.blockingResponse.remove(response.getKey());
    if (initReso.getStatus().equals(Checkpoint.FamilyInitializeResponse.Status.REJECTED)) {
      throw new JobFaultyException("Checkpointing initialization of "
          + family + " failed. CheckpointManager rejected the request due to cluster instability.");
    }
    return initReso;
  }

  @Override
  public void sendVersionUpdate(String family,
                                int index, long version, MessageHandler messageHandler) {
    RequestID requestID = this.rrClient.sendRequest(
        Checkpoint.VersionUpdateRequest.newBuilder()
            .setFamily(family)
            .setIndex(index)
            .setVersion(version)
            .build()
    );
    this.asyncHandlers.put(requestID, messageHandler);
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (this.asyncHandlers.containsKey(id)) {
      this.asyncHandlers.remove(id).onMessage(id, workerId, message);
    } else {
      this.blockingResponse.put(id, message);
    }
  }
}

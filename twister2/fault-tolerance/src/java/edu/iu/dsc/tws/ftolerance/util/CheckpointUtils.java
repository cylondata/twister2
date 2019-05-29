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
package edu.iu.dsc.tws.ftolerance.util;

import java.io.IOException;

import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.ftolerance.api.SnapshotImpl;
import edu.iu.dsc.tws.ftolerance.api.StateStore;

public final class CheckpointUtils {

  private CheckpointUtils() {
  }

  public static void commitState(StateStore stateStore,
                                 String family,
                                 int componentIndex,
                                 SnapshotImpl snapshot,
                                 CheckpointingClient checkpointingClient,
                                 MessageHandler messageHandler) throws IOException {
    stateStore.put(snapshot.getVersion().toString(), snapshot.pack());
    checkpointingClient.sendVersionUpdate(
        family,
        componentIndex,
        snapshot.getVersion(),
        messageHandler
    );
  }

  public static void restoreSnapshot(StateStore stateStore,
                                     Long version,
                                     SnapshotImpl snapshot) throws IOException {
    if (version == 0) {
      return;
    }
    byte[] stateBytes = stateStore.get(version.toString());
    snapshot.unpack(stateBytes);
  }
}

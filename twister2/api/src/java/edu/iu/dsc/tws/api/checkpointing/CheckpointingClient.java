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

package edu.iu.dsc.tws.api.checkpointing;

import java.util.Set;

import edu.iu.dsc.tws.api.exceptions.net.BlockingSendException;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

/**
 * todo This class was added as an temp solution to resolve a cyclic dependency issue.
 * This should be moved to fault tolerance package once code is refactored
 */
public interface CheckpointingClient {

  void sendVersionUpdate(String family,
                         int index, long version, MessageHandler messageHandler);

  Checkpoint.ComponentDiscoveryResponse sendDiscoveryMessage(
      String family, int index) throws BlockingSendException;

  Checkpoint.FamilyInitializeResponse initFamily(
      int containerIndex, int containersCount,
      String family, Set<Integer> members) throws BlockingSendException;
}

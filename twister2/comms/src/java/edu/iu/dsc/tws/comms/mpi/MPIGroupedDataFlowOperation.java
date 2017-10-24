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
package edu.iu.dsc.tws.comms.mpi;

import java.util.HashMap;
import java.util.Map;

public abstract class MPIGroupedDataFlowOperation extends MPIDataFlowOperation {
  /**
   * Keep track of the current message been received
   */
  protected Map<Integer, Map<Integer, MPIMessage>> groupedCurrentMessages = new HashMap<>();


  public MPIGroupedDataFlowOperation(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  protected void initSerializers() {
    messageDeSerializer.init(config, true);
    messageSerializer.init(config, true);
  }
}

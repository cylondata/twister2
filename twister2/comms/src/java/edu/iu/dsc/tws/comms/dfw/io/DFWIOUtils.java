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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.UUID;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;

public final class DFWIOUtils {

  private DFWIOUtils() {
    throw new UnsupportedOperationException();
  }

  public static String getOperationName(int target, DataFlowOperation op) {
    String uid = op.getUniqueId();
    return "partition-" + uid + "-" + target + "-" + UUID.randomUUID().toString();
  }

  public static boolean resetState(SerializeState state, boolean completed) {
    if (completed) {
      // add the key size at the end to total size
      state.setBytesCopied(0);
      state.setBufferNo(0);
      state.setData(null);
      state.setPart(SerializeState.Part.INIT);
      state.setKeySize(0);
      return true;
    } else {
      return false;
    }
  }
}

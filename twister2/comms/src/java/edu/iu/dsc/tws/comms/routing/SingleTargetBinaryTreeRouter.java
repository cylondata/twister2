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
package edu.iu.dsc.tws.comms.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SingleTargetBinaryTreeRouter implements IRouter {
  @Override
  public Set<Integer> receivingExecutors() {
    return null;
  }

  @Override
  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return null;
  }

  @Override
  public boolean isLast() {
    return false;
  }

  @Override
  public Set<Integer> getDownstreamTasks(int source) {
    return null;
  }

  @Override
  public int executor(int task) {
    return 0;
  }
}

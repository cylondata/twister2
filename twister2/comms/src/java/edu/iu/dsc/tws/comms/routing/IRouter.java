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

import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public interface IRouter {
  /**
   * Initialize the router
   *
   * @param cfg
   * @param plan
   * @param srscs
   * @param dests
   * @param strm
   * @param distinctRoutes
   */
  void init(Config cfg, TaskPlan plan, Set<Integer> srscs,
            Set<Integer> dests, int strm, int distinctRoutes);

  /**
   * For each source get a routing.
   *
   * @return a map of source to -> routing
   */
  Map<Integer, Routing> expectedRoutes();
}

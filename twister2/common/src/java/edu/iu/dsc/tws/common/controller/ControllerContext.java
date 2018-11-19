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
package edu.iu.dsc.tws.common.controller;

import edu.iu.dsc.tws.common.config.Config;

public final class ControllerContext {

  private ControllerContext() { }

  // max wait time to get worker list from the server in milliseconds
  public static final int MAX_WAIT_TIME_DEFAULT = 100000;
  public static final String MAX_WAIT_TIME_FOR_ALL_TO_JOIN =
      "twister2.worker.controller.max.wait.time.for.all.workers.to.join";

  public static final String MAX_WAIT_TIME_ON_BARRIER =
      "twister2.worker.controller.max.wait.time.on.barrier";

  public static int maxWaitTimeForAllToJoin(Config cfg) {
    return cfg.getIntegerValue(MAX_WAIT_TIME_FOR_ALL_TO_JOIN, MAX_WAIT_TIME_DEFAULT);
  }

  public static int maxWaitTimeOnBarrier(Config cfg) {
    return cfg.getIntegerValue(MAX_WAIT_TIME_ON_BARRIER, MAX_WAIT_TIME_DEFAULT);
  }

}

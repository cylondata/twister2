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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public final class MesosWorkerUtils {
  private static final Logger LOG = Logger.getLogger(MesosWorkerUtils.class.getName());

  private MesosWorkerUtils() {

  }
  /**
   * generate the additional requested ports for this worker
   * @param config
   * @param workerPort
   * @return
   */
  public static Map<String, Integer> generateAdditionalPorts(Config config, int workerPort) {
    // if no port is requested, return null
    List<String> portNames = SchedulerContext.additionalPorts(config);
    if (portNames == null) {
      return null;
    }
    HashMap<String, Integer> ports = new HashMap<>();
    int i = 1;
    for (String portName: portNames) {
      ports.put(portName, workerPort + i++);
    }
    return ports;
  }

}

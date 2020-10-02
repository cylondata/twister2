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

package edu.iu.dsc.tws.api.resource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

/**
 * A util class to initialize the Worker Network
 */
public final class Network {

  public static final Logger LOG = Logger.getLogger(Network.class.getName());

  private Network() {
  }

  public static TWSChannel initializeChannel(Config config, IWorkerController wController) {
    String channelClass = SchedulerContext.networkClass(config);
    try {
      return (TWSChannel) Network.class.getClassLoader()
          .loadClass(channelClass)
          .getConstructor(Config.class, IWorkerController.class)
          .newInstance(config, wController);
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | ClassNotFoundException e) {
      throw new Twister2RuntimeException("Couldn't initialize TWSChannel", e);
    }
  }

  /**
   * check whether a host is reachable
   * @param targetHostName
   * @return
   */
  public static boolean isReachable(String targetHostName) {
    InetAddress ip = null;
    try {
      ip = InetAddress.getByName(targetHostName);
    } catch (UnknownHostException e) {
      return false;
    }
    try {
      LOG.finest("Checking IP: " + targetHostName);
      return ip.isReachable(5000);
    } catch (IOException e) {
      return false;
    }
  }

}

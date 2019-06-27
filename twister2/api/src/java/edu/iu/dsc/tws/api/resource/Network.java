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

import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

/**
 * A util class to initialize the Worker Network
 */
public final class Network {

  public static final Logger LOG = Logger.getLogger(Network.class.getName());

  private Network() {
  }

  public static TWSChannel initializeChannel(Config config, IWorkerController wController) {
    String channelClass = config.getStringValue(
        "twister2.network.channel.class",
        "edu.iu.dsc.tws.comms.mpi.TWSMPIChannel");
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
}

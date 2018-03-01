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
package edu.iu.dsc.tws.comms.core;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.comms.mpi.TWSMPIChannel;

import mpi.MPI;

/**
 * Initialize the twister2 network code.
 * At this stage we expect to get the network configuration file from the configuration.
 */
public final class TWSNetwork {
  private static final Logger LOG = Logger.getLogger(TWSNetwork.class.getName());

  private Config config;

  private TWSCommunication dataFlowTWSCommunication;

  public TWSNetwork(Config cfg, TaskPlan taskPlan) {
    // load the network configuration
    this.config = loadConfig(cfg);

    // lets load the communication implementation
    String communicationClass = CommunicationContext.communicationClass(config);
    try {
      dataFlowTWSCommunication = ReflectionUtils.newInstance(communicationClass);
      LOG.log(Level.FINE, "Created communication with class: " + communicationClass);
      dataFlowTWSCommunication.init(config, taskPlan,
          new TWSMPIChannel(config, MPI.COMM_WORLD, taskPlan.getThisExecutor()));
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.severe("Failed to load the communications class: " + communicationClass);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the data flow channel
   *
   * @return the data flow channel
   */
  public TWSCommunication getDataFlowTWSCommunication() {
    return dataFlowTWSCommunication;
  }

  protected Config loadConfig(Config cfg) {
    String networkConfigFile = CommunicationContext.networkConfigurationFile(cfg);
    Config componentConfig = ConfigLoader.loadComponentConfig(networkConfigFile);
    return Config.newBuilder().putAll(cfg).putAll(componentConfig).build();
  }
}

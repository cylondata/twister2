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
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.mpi.TWSMPIChannel;
import edu.iu.dsc.tws.comms.tcp.TWSTCPChannel;

import mpi.MPI;

/**
 * Initialize the twister2 network code.
 * At this stage we expect to get the network configuration file from the configuration.
 */
public final class TWSNetwork {
  private static final Logger LOG = Logger.getLogger(TWSNetwork.class.getName());

  private Config config;

  private TWSCommunication dataFlowTWSCommunication;

  private TWSChannel channel;

  public TWSNetwork(Config cfg, int workerId) {
    // load the network configuration
    this.config = loadConfig(cfg);

    // lets load the communication implementation
    String communicationClass = CommunicationContext.communicationClass(config);
    try {
      dataFlowTWSCommunication = ReflectionUtils.newInstance(communicationClass);
      LOG.log(Level.FINE, "Created communication with class: " + communicationClass);
      this.channel = new TWSMPIChannel(config, MPI.COMM_WORLD, workerId);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.severe("Failed to load the communications class: " + communicationClass);
      throw new RuntimeException(e);
    }
  }

  public TWSNetwork(Config cfg, int workerId, NetworkInfo networkInfo) {
    // load the network configuration
    this.config = loadConfig(cfg);

    // lets load the communication implementation
    String communicationClass = CommunicationContext.communicationClass(config);
    try {
      dataFlowTWSCommunication = ReflectionUtils.newInstance(communicationClass);
      LOG.log(Level.FINE, "Created communication with class: " + communicationClass);
      String commType = CommunicationContext.communicationType(config);
      if (CommunicationContext.MPI_COMMUNICATION_TYPE.equals(commType)) {
        this.channel = new TWSMPIChannel(config, MPI.COMM_WORLD, workerId);
      } else if (CommunicationContext.TCP_COMMUNICATION_TYPE.equals(commType)) {
        this.channel = new TWSTCPChannel(config, workerId, new TCPChannel(config, networkInfo));
      }
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.severe("Failed to load the communications class: " + communicationClass);
      throw new RuntimeException(e);
    }
  }

  public TWSNetwork(Config cfg, TaskPlan taskPlan) {
    // load the network configuration
    this.config = loadConfig(cfg);

    // lets load the communication implementation
    String communicationClass = CommunicationContext.communicationClass(config);
    try {
      dataFlowTWSCommunication = ReflectionUtils.newInstance(communicationClass);
      LOG.log(Level.FINE, "Created communication with class: " + communicationClass);
      this.channel = new TWSMPIChannel(config, MPI.COMM_WORLD, taskPlan.getThisExecutor());
      dataFlowTWSCommunication.init(config, taskPlan, channel);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.severe("Failed to load the communications class: " + communicationClass);
      throw new RuntimeException(e);
    }
  }

  public TWSNetwork(Config cfg, TWSChannel ch, TaskPlan taskPlan) {
    // load the network configuration
    this.config = loadConfig(cfg);

    // lets load the communication implementation
    String communicationClass = CommunicationContext.communicationClass(config);
    try {
      dataFlowTWSCommunication = ReflectionUtils.newInstance(communicationClass);
      LOG.log(Level.FINE, "Created communication with class: " + communicationClass);
      dataFlowTWSCommunication.init(config, taskPlan, ch);
      this.channel = ch;
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.severe("Failed to load the communications class: " + communicationClass);
      throw new RuntimeException(e);
    }
  }

  public TWSChannel getChannel() {
    return channel;
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

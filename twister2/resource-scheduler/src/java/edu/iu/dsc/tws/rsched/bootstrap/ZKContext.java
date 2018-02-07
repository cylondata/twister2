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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public final class ZKContext {
  public static final Logger LOG = Logger.getLogger(ZKContext.class.getName());

  public static final String ROOT_NODE = "twister2.zookeeper.root.node.path";
  public static final String ROOT_NODE_DEFAULT = "/twister2";

  // ZooKeeper server IP address and port number
  // They should be given in conf files
  public static final String ZOOKEEPER_SERVER_IP = "twister2.zookeeper.server.ip";
  public static final String ZOOKEEPER_SERVER_PORT = "twister2.zookeeper.server.port";
  public static final String MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN =
      "twister2.zookeeper.max.wait.time.for.all.workers.to.join";

  private ZKContext() { }

  public static String rootNode(Config cfg) {
    return cfg.getStringValue(ROOT_NODE, ROOT_NODE_DEFAULT);
  }

  public static String zooKeeperServerIP(Config cfg) {
    return cfg.getStringValue(ZOOKEEPER_SERVER_IP);
  }

  public static String zooKeeperServerPort(Config cfg) {
    return cfg.getStringValue(ZOOKEEPER_SERVER_PORT);
  }

  public static int maxWaitTimeForAllWorkersToJoin(Config cfg) {
    return Integer.parseInt(cfg.getStringValue(MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN));
  }


}

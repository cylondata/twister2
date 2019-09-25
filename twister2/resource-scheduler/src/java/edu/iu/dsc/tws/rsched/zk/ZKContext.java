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

package edu.iu.dsc.tws.rsched.zk;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;

public final class ZKContext extends Context {
  public static final Logger LOG = Logger.getLogger(ZKContext.class.getName());

  public static final String ROOT_NODE = "twister2.resource.zookeeper.root.node.path";
  public static final String ROOT_NODE_DEFAULT = "/twister2";

  // comma separated ZooKeeper server IP:port pairs
  // example: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  public static final String ZOOKEEPER_SERVER_ADDRESSES
      = "twister2.resource.zookeeper.server.addresses";

  // SESSION_TIMEOUT determines worker failures.
  // If workers do not send heartbeat messages for this many milli seconds,
  // they are assumed failed
  public static final String SESSION_TIMEOUT = "twister2.resource.zookeeper.session.timeout";
  public static final int SESSION_TIMEOUT_DEFAULT = 10 * 1000;

  private ZKContext() { }

  public static String rootNode(Config cfg) {
    return cfg.getStringValue(ROOT_NODE, ROOT_NODE_DEFAULT);
  }

  public static String zooKeeperServerAddresses(Config cfg) {
    return cfg.getStringValue(ZOOKEEPER_SERVER_ADDRESSES);
  }

  public static int sessionTimeout(Config cfg) {
    return cfg.getIntegerValue(SESSION_TIMEOUT, SESSION_TIMEOUT_DEFAULT);
  }

}

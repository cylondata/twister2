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
import edu.iu.dsc.tws.common.config.Context;

public final class ZKContext extends Context {
  public static final Logger LOG = Logger.getLogger(ZKContext.class.getName());

  public static final String ROOT_NODE = "twister2.zookeeper.root.node.path";
  public static final String ROOT_NODE_DEFAULT = "/twister2";

  // comma separated ZooKeeper server IP:port pairs
  // example: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  public static final String ZOOKEEPER_SERVER_ADDRESSES = "twister2.zookeeper.server.addresses";

  public static final int MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN_DEFAULT = 100000;
  public static final String MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN =
      "twister2.zookeeper.max.wait.time.for.all.workers.to.join";

  private ZKContext() { }

  public static String rootNode(Config cfg) {
    return cfg.getStringValue(ROOT_NODE, ROOT_NODE_DEFAULT);
  }

  public static String zooKeeperServerAddresses(Config cfg) {
    return cfg.getStringValue(ZOOKEEPER_SERVER_ADDRESSES);
  }

  public static int maxWaitTimeForAllWorkersToJoin(Config cfg) {
    return cfg.getIntegerValue(MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN,
        MAX_WAIT_TIME_FOR_ALL_WORKERS_TO_JOIN_DEFAULT);
  }


}

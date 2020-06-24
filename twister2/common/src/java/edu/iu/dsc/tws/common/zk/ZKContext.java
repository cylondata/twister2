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

package edu.iu.dsc.tws.common.zk;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;

public final class ZKContext extends Context {

  public static final String ROOT_NODE = "twister2.resource.zookeeper.root.node.path";
  public static final String ROOT_NODE_DEFAULT = "/twister2";

  // comma separated ZooKeeper server IP:port pairs
  // example: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
  public static final String SERVER_ADDRESSES = "twister2.resource.zookeeper.server.addresses";

  private ZKContext() { }

  public static String rootNode(Config cfg) {
    return cfg.getStringValue(ROOT_NODE, ROOT_NODE_DEFAULT);
  }

  public static String serverAddresses(Config cfg) {
    return cfg.getStringValue(SERVER_ADDRESSES);
  }

  /**
   * return true if the job is fault tolerant or ZK based group management is used
   * @param cfg
   * @return
   */
  public static boolean isZooKeeperServerUsed(Config cfg) {

    if (serverAddresses(cfg) != null) {
      return true;
    }

    return false;
  }



}

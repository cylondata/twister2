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

import java.nio.ByteOrder;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class CommunicationContext extends Context {
  public static final String DATAFLOW_COMMUNICATION_CLASS = "twister2.network.dataflow.class";
  private static final String INTER_NODE_DEGREE = "network.routing.inter.node.degree";
  private static final String INTRA_NODE_DEGREE = "network.routing.intra.node.degree";
  public static final ByteOrder DEFAULT_BYTEORDER = ByteOrder.BIG_ENDIAN;

  public static String communicationClass(Config cfg) {
    return cfg.getStringValue(DATAFLOW_COMMUNICATION_CLASS);
  }

  public static int interNodeDegree(Config cfg, int defaultValue) {
    return cfg.getIntegerValue(INTER_NODE_DEGREE, defaultValue);
  }

  public static int intraNodeDegree(Config cfg, int defaultValue) {
    return cfg.getIntegerValue(INTRA_NODE_DEGREE, defaultValue);
  }
}

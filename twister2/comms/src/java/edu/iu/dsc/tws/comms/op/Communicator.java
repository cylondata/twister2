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
package edu.iu.dsc.tws.comms.op;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;

/**
 * Communicator that keeps the information about.
 */
public class Communicator {
  private static final Logger LOG = Logger.getLogger(Communicator.class.getName());

  private final TWSChannel channel;

  private final Config config;

  private EdgeGenerator edgeGenerator;

  public Communicator(Config cfg, TWSChannel ch) {
    this.channel = ch;
    this.config = cfg;
    this.edgeGenerator = new EdgeGenerator(0);
  }

  public TWSChannel getChannel() {
    return channel;
  }

  public Config getConfig() {
    return config;
  }

  public int nextEdge() {
    return edgeGenerator.nextEdge();
  }
}

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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.CommunicationContext;

/**
 * Communicator that keeps the information about.
 */
public class Communicator {
  private static final Logger LOG = Logger.getLogger(Communicator.class.getName());

  /**
   * Communication channel
   */
  private final TWSChannel channel;

  /**
   * Configuration
   */
  private final Config config;

  /**
   * Generating edges
   */
  private EdgeGenerator edgeGenerator;

  /**
   * Generating task ids
   */
  private TaskIdGenerator idGenerator;

  /**
   * The directory to use for persisting any operations
   */
  private String persistentDirectory = "/tmp";

  public Communicator(Config cfg, TWSChannel ch) {
    this(cfg, ch, null);
  }

  public Communicator(Config config, TWSChannel ch, String persDir) {
    this.channel = ch;
    this.config = config;
    // first lets try to retrieve through a config
    if (persDir == null) {
      this.persistentDirectory = CommunicationContext.persistentDirectory(config);
    } else {
      this.persistentDirectory = persDir;
    }
    LOG.log(Level.INFO, String.format("Using the persistent directory %s", persistentDirectory));
    this.edgeGenerator = new EdgeGenerator(0);
    this.idGenerator = new TaskIdGenerator(100000000);
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

  public int nextId() {
    return idGenerator.nextId();
  }

  public String getPersistentDirectory() {
    return persistentDirectory;
  }

  /**
   * Terminate the communicator
   */
  public void close() {
    channel.close();
  }
}

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
package edu.iu.dsc.tws.comms.api;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

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
  private String persistentDirectory;

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
    LOG.log(Level.FINE, String.format("Using the persistent directory %s", persistentDirectory));
    this.edgeGenerator = new EdgeGenerator(0);
    this.idGenerator = new TaskIdGenerator(100000000);
  }

  protected Communicator(Config config, TWSChannel channel, EdgeGenerator edgeGenerator,
                         TaskIdGenerator idGenerator, String persistentDirectory) {
    this.channel = channel;
    this.config = config;
    this.edgeGenerator = edgeGenerator;
    this.idGenerator = idGenerator;
    this.persistentDirectory = persistentDirectory;
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

  /**
   * Create a communicator with new configuration
   *
   * @param newConfig the new configuration
   * @return communicator
   */
  public Communicator newWithConfig(Map<String, Object> newConfig) {
    return new Communicator(
        Config.newBuilder().putAll(config).putAll(newConfig).build(), channel,
        edgeGenerator, idGenerator, persistentDirectory);
  }
}

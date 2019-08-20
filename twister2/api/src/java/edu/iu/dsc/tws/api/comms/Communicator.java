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

package edu.iu.dsc.tws.api.comms;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;

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
  private Config config;

  /**
   * Generating edges
   */
  private EdgeGenerator edgeGenerator;

  /**
   * Generating task ids
   */
  private TaskIdGenerator idGenerator;

  private List<String> persistentDirectories;

  public Communicator(Config cfg, TWSChannel ch) {
    this(cfg, ch, (List<String>) null);
  }

  public Communicator(Config config, TWSChannel ch, List<String> persDirs) {
    this.channel = ch;
    this.config = config;
    // first lets try to retrieve through a config
    if (persDirs == null) {
      this.persistentDirectories = CommunicationContext.persistentDirectory(config);
      if (this.persistentDirectories.size() > 0) {
        LOG.log(Level.FINE, "The persistence operations will be load balanced between : "
            + this.persistentDirectories);
      }
    } else {
      this.persistentDirectories = persDirs;
    }
    LOG.log(Level.FINE, String.format("Using the persistent directories %s",
        persistentDirectories));
    this.edgeGenerator = new EdgeGenerator(0);
    this.idGenerator = new TaskIdGenerator(100000000);
  }

  public Communicator(Config config, TWSChannel ch, String persDir) {
    this(config, ch, persDir != null ? Collections.singletonList(persDir) : null);
  }

  protected Communicator(Config config, TWSChannel channel, EdgeGenerator edgeGenerator,
                         TaskIdGenerator idGenerator, List<String> persistentDirectories) {
    this.channel = channel;
    this.config = config;
    this.edgeGenerator = edgeGenerator;
    this.idGenerator = idGenerator;
    this.persistentDirectories = persistentDirectories;
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

  public String getPersistentDirectory(int requesterId) {
    int dirIndex = this.persistentDirectories.size() % requesterId;
    return this.persistentDirectories.get(dirIndex);
  }

  public List<String> getPersistentDirectories() {
    return persistentDirectories;
  }

  /**
   * Terminate the communicator
   */
  public void close() {
    channel.close();
  }

  /**
   * Update the configs with new configurations
   * @param newConfigs the new configurations
   */
  public void updateConfig(Map<String, Object> newConfigs) {
    this.config = Config.newBuilder().putAll(config).putAll(newConfigs).build();
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
        edgeGenerator, idGenerator, persistentDirectories);
  }
}

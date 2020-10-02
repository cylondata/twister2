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

package edu.iu.dsc.tws.common.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.util.KryoSerializer;

public final class ConfigLoader {

  private static final Logger LOG = Logger.getLogger(ConfigLoader.class.getName());

  private ConfigLoader() {
  }

  private static Config loadDefaults(String twister2Home, String configPath) {
    String home = System.getProperty("user.home");
    return Config.newBuilder()
        .putAll(Context.getDefaults())
        .put(Context.HOME.getKey(), home)
        .put(Context.TWISTER2_HOME.getKey(), twister2Home)
        .put(Context.TWISTER2_CONF.getKey(), configPath)
        .build();
  }

  private static Config loadConfig(String file) {
    Map<String, Object> readConfig = ConfigReader.loadFile(file);
    return addFromFile(readConfig);
  }

  private static Config addFromFile(Map<String, Object> readConfig) {
    return Config.newBuilder().putAll(readConfig).build();
  }

  /**
   * Loads raw configurations from files under the twister2 Home and configPath. The returned config
   * must be converted to either local or cluster mode to trigger pattern substitution of wildcards
   * tokens.
   */
  public static Config loadConfig(String twister2Home,
                                  String configPath) {
    Config defaultConfig = loadDefaults(twister2Home, configPath);
    // to token-substitute the conf paths
    Config localConfig = Config.transform(defaultConfig);

    // now load the configurations
    Config.Builder cb = Config.newBuilder()
        .putAll(localConfig)
        .putAll(loadConfig(Context.taskConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.resourceSchedulerConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.networkConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.systemConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.dataConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.checkpointConfigurationFile(localConfig)));

    Config config = cb.build();
    return Config.transform(config);
  }

  public static Config loadTestConfig() {
    //copying resources
    File root = new File("twister2");
    String[] filesList = new String[]{
        "core.yaml",
        "network.yaml",
        "data.yaml",
        "resource.yaml",
        "task.yaml",
        "logger.properties"
    };
    File commonConf = new File(root, "conf/common");
    commonConf.mkdirs();
    for (String configFile : filesList) {
      try {
        Path configPath = Paths.get(
            commonConf.getAbsolutePath(),
            configFile
        );
        if (!Files.exists(configPath)) {
          Files.copy(ConfigLoader.class.getResourceAsStream("/" + configFile), configPath);
        }
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Couldn't copy files", e);
      }
    }


    String twister2Home = root.getAbsolutePath();
    String configDir = commonConf.getParent();
    String clusterType = "standalone";
    return loadConfig(twister2Home, configDir, clusterType);
  }

  public static Config loadConfig(String twister2Home, String configPathRoot,
                                  String clusterType) {
    //load from common
    Config common = loadConfig(twister2Home, configPathRoot
        + File.separator + "common");
    common = Config.newBuilder().putAll(common).put(Context.TWISTER2_COMMON_CONF_DIR, configPathRoot
        + File.separator + "common").build();

    //platform specific config
    Config platformSpecific = loadConfig(twister2Home, configPathRoot
        + File.separator + clusterType);


    return Config.newBuilder().putAll(common).putAll(platformSpecific).build();
  }

  public static Config loadComponentConfig(String filePath) {
    Config.Builder cb = Config.newBuilder()
        .putAll(loadConfig(filePath));
    return Config.transform(cb.build());
  }

  public static Config loadConfig(byte[] serializedMap) {
    return Config.newBuilder().putAll(
        (Map) new KryoSerializer().deserialize(serializedMap)).build();
  }
}

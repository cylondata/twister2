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

import java.util.Map;

public final class ConfigLoader {
  private ConfigLoader() {
  }

  private static Config loadDefaults(String twister2Home, String configPath) {
    String home = System.getProperty("user.home");
    return Config.newBuilder()
        .putAll(Context.defaults)
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
        .putAll(loadConfig(Context.clientConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.taskConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.resourceSchedulerConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.uploaderConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.networkConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.systemConfigurationFile(localConfig)))
        .putAll(loadConfig(Context.dataConfigurationFile(localConfig)));
    Config config = cb.build();
    return Config.transform(config);
  }

  public static Config loadComponentConfig(String filePath) {
    Config.Builder cb = Config.newBuilder()
        .putAll(loadConfig(filePath));
    return Config.transform(cb.build());
  }
}

//  Copyright 2017 Twitter. All rights reserved.
//
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Config is an Immutable Map of &lt;String, Object&gt; The get/set API that uses Key objects
 * should be favored over Strings. Usage of the String API should be refactored out.
 */
public class Config {
  private static final Logger LOG = Logger.getLogger(Config.class.getName());

  private final Map<String, Object> cfgMap;

  private final Mode mode;
  private final Config initialConfig;     // what the user first creates
  private Config transformedConfig = null;  // what gets generated after transformations

  private enum Mode {
    INITIAL,    // the initially provided configs without pattern substitution
    TRANSFORMED,  // the provided configs with pattern substitution for the client env
  }

  // Used to initialize a raw config. Should be used by consumers of Config via the builder
  protected Config(Builder build) {
    this.mode = Mode.INITIAL;
    this.initialConfig = this;
    this.cfgMap = new HashMap<>(build.keyValues);
  }

  // Used internally to create a Config that is actually a facade over a raw, local and
  // cluster config
  private Config(Mode mode, Config initialConfig, Config newConfig) {
    this.mode = mode;
    this.initialConfig = initialConfig;
    this.transformedConfig = newConfig;
    switch (mode) {
      case INITIAL:
        this.cfgMap = initialConfig.cfgMap;
        break;
      case TRANSFORMED:
        this.cfgMap = transformedConfig.cfgMap;
        break;
      default:
        throw new IllegalArgumentException("Unrecognized mode passed to constructor: " + mode);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Transform with default substitutions as in @Context
   *
   * @param config the intial configuration
   * @return a new configuration
   */
  public static Config transform(Config config) {
    return config.lazyCreateConfig(Mode.TRANSFORMED, Context.substitutions);
  }

  public static Config transform(Config config, Map<String, ConfigEntry> substitutions) {
    return config.lazyCreateConfig(Mode.TRANSFORMED, substitutions);
  }

  public Map<String, Object> toMap() {
    return new HashMap<>(cfgMap);
  }

  /**
   * Recursively expand each config value until token substitution is exhausted. We must recurse
   * to handle the case where field expansion requires multiple iterations, due to new tokens being
   * introduced as we replace. For example:
   *
   *   ${TWISTER2_BIN}/twister2-executor        gets expanded to
   *   ${TWISTER2_HOME}/bin/twister2-executor   gets expanded to
   *   /usr/local/twister2/bin/twister2-executor
   *
   * If break logic is when another round does not reduce the number of tokens, since it means we
   * couldn't find a valid replacement.
   */
  private static Config expand(Config config, int previousTokensCount,
                               Map<String, ConfigEntry> substitutions) {
    Config.Builder cb = Config.newBuilder().putAll(config);
    int tokensCount = 0;
    for (String key : config.getKeySet()) {
      Object value = config.get(key);
      if (value instanceof String) {
        String expandedValue = TokenSub.substitute(config, (String) value, substitutions);
        if (expandedValue.contains("${")) {
          tokensCount++;
        }
        cb.put(key, expandedValue);
      } else {
        cb.put(key, value);
      }
    }

    // go recursive if required
    if (previousTokensCount != tokensCount) {
      return expand(cb.build(), tokensCount, substitutions);
    } else {
      return cb.build();
    }
  }

  private Config lazyCreateConfig(Mode newMode, Map<String, ConfigEntry> subtitutions) {
    // this is here so that we don't keep cascading deeper into object creation so:
    // localConfig == toLocalMode(toClusterMode(localConfig))
    Config newInitialConfig = this.initialConfig;
    Config newTransformedConfig = this.transformedConfig;
    switch (this.mode) {
      case INITIAL:
        newInitialConfig = this;
        break;
      case TRANSFORMED:
        newTransformedConfig = this;
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized mode found in config: " + this.mode);
    }

    switch (newMode) {
      case TRANSFORMED:
        if (this.transformedConfig == null) {
          Config tempConfig = expand(
              Config.newBuilder().putAll(initialConfig.cfgMap).build(), 0, subtitutions);
          this.transformedConfig = new Config(Mode.TRANSFORMED, newInitialConfig, tempConfig);
        }
        return this.transformedConfig;
      case INITIAL:
      default:
        throw new IllegalArgumentException(
            "Unrecognized mode passed to lazyCreateConfig: " + newMode);
    }
  }

  public int size() {
    return cfgMap.size();
  }

  public Object get(String key) {
    switch (mode) {
      case TRANSFORMED:
        return transformedConfig.cfgMap.get(key);
      case INITIAL:
        return initialConfig.cfgMap.get(key);
      default:
        throw new IllegalArgumentException(String.format(
            "Unrecognized mode passed to get for key=%s: %s", key, mode));
    }
  }

  public String getStringValue(ConfigEntry key) {
    return getStringValue(key.getKey(), key.getDefaultValue());
  }

  public String getStringValue(String key) {
    return (String) get(key);
  }

  public String getStringValue(String key, String defaultValue) {
    String value = getStringValue(key);
    return value != null ? value : defaultValue;
  }

  @SuppressWarnings("unchecked")
  public List<String> getListValue(String key) {
    return (List) get(key);
  }

  public List<String> getListValue(String key, List<String> defaultValue) {
    List<String> value = getListValue(key);
    return value != null ? value : defaultValue;
  }

  @SuppressWarnings("unchecked")
  public List<Map<String, List<String>>> getListOfMapsWithListValues(String key) {
    return (List<Map<String, List<String>>>) get(key);
  }

  public Boolean getBooleanValue(String key) {
    Object value = get(key);
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      String strValue = (String) value;
      if ("true".equalsIgnoreCase(strValue)) {
        return true;
      } else if ("false".equalsIgnoreCase(strValue)) {
        return false;
      }
    }

    return null;
  }

  public Boolean getBooleanValue(String key, boolean defaultValue) {
    Boolean value = getBooleanValue(key);
    return value != null ? value : defaultValue;
  }

  public Long getLongValue(String key, long defaultValue) {
    Object value = get(key);
    if (value != null) {
      return TypeUtils.getLong(value);
    }
    return defaultValue;
  }

  public Integer getIntegerValue(String key, int defaultValue) {
    Object value = get(key);
    if (value != null) {
      if (value instanceof Integer) {
        return (Integer) value;
      } else if (value instanceof String) {
        return Integer.valueOf((String) value);
      } else {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  public Double getDoubleValue(String key, double defaultValue) {
    Object value = get(key);
    if (value != null) {
      if (value instanceof Integer) {
        return ((Integer) value).doubleValue();
      } else if (value instanceof Double) {
        return (Double) value;
      } else if (value instanceof String) {
        return Double.valueOf((String) value);
      } else {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  @SuppressWarnings("unchecked")
  public List<String> getStringList(String key) {
    Object value = get(key);
    if (value instanceof List<?>) {
      return (List<String>) value;
    } else {
      return null;
    }
  }


  public Set<String> getKeySet() {
    return cfgMap.keySet();
  }

  @Override
  public String toString() {
    Map<String, Object> treeMap = new TreeMap<>(cfgMap);
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> entry : treeMap.entrySet()) {
      sb.append("(\"").append(entry.getKey()).append("\"");
      sb.append(", ").append(entry.getValue()).append(")\n");
    }
    return sb.toString();
  }

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<>();

    public Builder put(String key, Object value) {
      this.keyValues.put(key, value);
      return this;
    }

    public Builder putAll(Config ctx) {
      keyValues.putAll(ctx.cfgMap);
      return this;
    }

    public Builder putAll(Map<String, Object> map) {
      keyValues.putAll(map);
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }
}

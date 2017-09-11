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

public class ConfigEntry {
  public enum Type {
    BOOLEAN,
    BYTE_AMOUNT,
    DOUBLE,
    DRY_RUN_FORMAT_TYPE,
    INTEGER,
    LONG,
    STRING,
    PACKAGE_TYPE,
    PROPERTIES,
    UNKNOWN
  }

  private String key;
  private String defaultValue;
  private Type type;

  public ConfigEntry(String key, String defaultValue, Type type) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.type = type;
  }

  public ConfigEntry(String key) {
    this(key, null, Type.STRING);
  }

  public ConfigEntry(String key, Type type) {
    this(key, null, type);
  }

  public ConfigEntry(String key, String defaultValue) {
    this(key, defaultValue, Type.STRING);
  }

  public String getKey() {
    return key;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public Type getType() {
    return type;
  }
}

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
  private String substitute;
  private Type type;

  public ConfigEntry(String key, String defaultValue, Type type, String substitute) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.type = type;
    this.substitute = substitute;
  }

  public ConfigEntry(String key) {
    this(key, null, Type.STRING, null);
  }

  public ConfigEntry(String key, Type type) {
    this(key, null, type, null);
  }

  public ConfigEntry(String key, String defaultValue) {
    this(key, defaultValue, Type.STRING, null);
  }

  public ConfigEntry(String key, String defaultValue, String substitute) {
    this(key, defaultValue, Type.STRING, substitute);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConfigEntry that = (ConfigEntry) o;

    return key != null ? key.equals(that.key) : that.key == null;

  }

  @Override
  public int hashCode() {
    return key != null ? key.hashCode() : 0;
  }
}

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
package edu.iu.dsc.tws.api.tset.sources;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

/**
 * Hadoop configuration is no serializable, so we created this
 * wrapper to serialize it manually
 */
public class HadoopConfSerializeWrapper implements Serializable {
  private transient Configuration configuration;

  /**
   * Holds the serialize value
   */
  private byte[] value;

  public HadoopConfSerializeWrapper(Configuration configuration) {
    this.configuration = configuration;

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      configuration.write(new DataOutputStream(out));

      this.value = out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to write hadoop configuration to byte array", e);
    }
  }

  public Configuration getConfiguration() {
    if (configuration == null) {
      if (value == null) {
        throw new RuntimeException("Value cannot be null");
      }

      configuration = new Configuration();
      try {
        configuration.readFields(new DataInputStream(new ByteArrayInputStream(value)));
      } catch (IOException e) {
        throw new RuntimeException("Failed to read hadoop configuration from byte array", e);
      }
    }
    return configuration;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }
}

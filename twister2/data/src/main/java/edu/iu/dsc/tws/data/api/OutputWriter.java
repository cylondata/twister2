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
package edu.iu.dsc.tws.data.api;

import java.io.Serializable;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Output writer to write a output
 */
public interface OutputWriter<T> extends Serializable {
  /**
   * Configure the output writer
   *
   * @param config configuration
   */
  void configure(Config config);

  /**
   * Add value to output
   */
  void write(int partition, T out);

  /**
   * Close the writer, flush everything
   */
  void close();
}

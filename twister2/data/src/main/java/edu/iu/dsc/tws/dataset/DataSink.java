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
package edu.iu.dsc.tws.dataset;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.OutputWriter;

/**
 * A data sink
 *
 * @param <T>
 */
public class DataSink<T> {
  private static final Logger LOG = Logger.getLogger(DataSink.class.getName());

  private OutputWriter<T> outputWriter;

  public DataSink(Config config, DataSet<T> dataSet) {
  }

  public DataSink(Config config, OutputWriter<T> output) {
    this.outputWriter = output;
    this.outputWriter.configure(config);
  }

  public void add(int partition, T value) {
    outputWriter.write(partition, value);
  }

  public void persist() {
    outputWriter.close();
  }
}

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

import java.io.IOException;
import java.io.Serializable;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.fs.io.InputSplitter;

/**
 * The base interface for data sources that produces records.
 * <p>
 * The input format handles the following:
 * <ul>
 * <li>It describes how the input is split into splits that can be processed in parallel.</li>
 * <li>It describes how to read records from the input split.</li>
 * <li>It describes how to gather basic statistics from the input.</li>
 * </ul>
 * <p>
 * The life cycle of an input format is the following:
 * <ol>
 * // *   <li>After being instantiated (parameterless), it is configured with a Configuration object.
 * Basic fields are read from the configuration, such as for example a file path,
 * if the format describes
 * files as input.</li>
 * <li>Optionally: It is called by the compiler to produce basic statistics about the input.</li>
 * <li>It is called to create the input splits.</li>
 * <li>Each parallel input task creates an instance, configures it and opens it for a
 * specific split.</li>
 * <li>All records are read from the input</li>
 * <li>The input format is closed</li>
 * </ol>
 * <p>
 * IMPORTANT NOTE: Input formats must be written such that an instance can be opened
 * again after it was closed. That
 * is due to the fact that the input format is used for potentially multiple splits.
 * After a split is done, the
 * format's close function is invoked and, if another split is available, the open
 * function is invoked afterwards for
 * the next split.
 *
 * @param <OT> The type of the produced records.
 * @param <T> The type of input split.
 * @see InputSplit
 * //@see BaseStatistics
 */
public interface InputFormat<OT, T extends InputSplit> extends InputSplitter<T>, Serializable {

  /**
   * Configures this input format. Since input formats are instantiated generically
   * and hence parameterless, this method is the place where the input formats
   * set their basic fields based on configuration values.
   * <p>
   * This method is always called first on a newly instantiated input format.
   *
   * @param parameters The configuration with all parameters
   * (note: not the Flink config but the TaskConfig).
   */
  void configure(Config parameters);

  @Override
  T[] createInputSplits(int minNumSplits) throws Exception;

  @Override
  InputSplitAssigner getInputSplitAssigner(T[] inputSplits);

  /**
   * Opens a parallel instance of the input format to work on a split.
   * <p>
   * When this method is called, the input format it guaranteed to be configured.
   *
   * @param split The split to be opened.
   * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
   */
  void open(T split) throws IOException;

  /**
   * Method used to check if the end of the input is reached.
   * <p>
   * When this method is called, the input format it guaranteed to be opened.
   *
   * @return True if the end is reached, otherwise false.
   * @throws IOException Thrown, if an I/O error occurred.
   */
  boolean reachedEnd() throws IOException;

  /**
   * Reads the next record from the input.
   * <p>
   * When this method is called, the input format it guaranteed to be opened.
   *
   * @param reuse Object that may be reused.
   * @return Read record.
   * @throws IOException Thrown, if an I/O error occurred.
   */
  OT nextRecord(OT reuse) throws IOException;

  /**
   * Method that marks the end of the life-cycle of an input split. Should be used to
   * close channels and streams and release resources. After this method returns without an error,
   * the input is assumed to be correctly read.
   * <p>
   * When this method is called, the input format it guaranteed to be opened.
   *
   * @throws IOException Thrown, if the input could not be closed properly.
   */
  void close() throws IOException;
}

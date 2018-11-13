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

package edu.iu.dsc.tws.data.fs.io;

import java.io.IOException;
import java.io.Serializable;

import edu.iu.dsc.tws.common.config.Config;

/**
 * This interface must be implemented by all kind of input splits that
 * can be assigned to input formats.
 * <p>
 * <p>Input splits are transferred in serialized form via the messages,
 * so they need to be serializable as defined by {@link Serializable}.
 * </p>
 */
public interface InputSplit<OT> extends Serializable {

  void configure(Config parameters);

  /**
   * Returns the number of this input split.
   *
   * @return the number of this input split
   */
  int getSplitNumber();

  /**
   * Opens a parallel instance of the input format to work on a split.
   * <p>
   * When this method is called, the input format it guaranteed to be configured.
   *
   * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
   */
  void open() throws IOException;

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

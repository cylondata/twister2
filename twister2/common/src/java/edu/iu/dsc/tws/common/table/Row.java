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
package edu.iu.dsc.tws.common.table;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

/**
 * Represent data as a collection of rows.
 */
public interface Row {
  /**
   * Return the number of columns
   *
   * @return columns
   */
  default int numberOfColumns() {
    return this.getData().length;
  }

  /**
   * Create a duplicate of the row
   *
   * @return a copy of the row
   */
  Row duplicate();

  /**
   * Get the value in the column
   *
   * @param column column index
   * @return the value of the column
   */
  default Object get(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the string value in the column, if not a string value throw a runtime exception
   *
   * @param column column index
   * @return the string value
   */
  default String getString(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the int value in the column, if not a int value throw a runtime exception
   *
   * @param column column index
   * @return the int value
   */
  default int getInt4(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the long value in the column, if not a long value throw a runtime exception
   *
   * @param column column index
   * @return the long value
   */
  default long getInt8(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the double value in the column, if not a double value throw a runtime exception
   *
   * @param column column index
   * @return the double value
   */
  default double getFloat8(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the float value in the column, if not a float value throw a runtime exception
   *
   * @param column column index
   * @return the float value
   */
  default float getFloat4(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the short value in the column, if not a short value throw a runtime exception
   *
   * @param column column index
   * @return the short value
   */
  default short getInt2(int column) {
    return this.getTypedData(column);
  }

  /**
   * Get the byte value in the column, if not a byte value throw a runtime exception
   *
   * @param column column index
   * @return the byte value
   */
  default byte[] getByte(int column) {
    return this.getTypedData(column);
  }

  Object[] getData();

  default <T> T getTypedData(int column) {
    if (column <= this.getData().length) {
      return (T) this.getData()[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }
}

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

/**
 * Represent data as a collection of rows.
 */
public interface Row {
  /**
   * Return the number of columns
   * @return columns
   */
  int numberOfColumns();

  /**
   * Create a duplicate of the row
   * @return a copy of the row
   */
  Row duplicate();

  /**
   * Get the value in the column
   * @param column column index
   * @return the value of the column
   */
  Object get(int column);

  /**
   * Get the string value in the column, if not a string value throw a runtime exception
   * @param column column index
   * @return the string value
   */
  String getString(int column);

  /**
   * Get the int value in the column, if not a int value throw a runtime exception
   * @param column column index
   * @return the int value
   */
  int getInt4(int column);

  /**
   * Get the long value in the column, if not a long value throw a runtime exception
   * @param column column index
   * @return the long value
   */
  long getInt8(int column);

  /**
   * Get the double value in the column, if not a double value throw a runtime exception
   * @param column column index
   * @return the double value
   */
  double getFloat8(int column);

  /**
   * Get the float value in the column, if not a float value throw a runtime exception
   * @param column column index
   * @return the float value
   */
  float getFloat4(int column);

  /**
   * Get the short value in the column, if not a short value throw a runtime exception
   * @param column column index
   * @return the short value
   */
  short getInt2(int column);

  /**
   * Get the byte value in the column, if not a byte value throw a runtime exception
   * @param column column index
   * @return the byte value
   */
  byte[] getByte(int column);
}

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
package edu.iu.dsc.tws.data.api.formatters;

import java.util.logging.Logger;

import edu.iu.dsc.tws.data.api.InputFormat;

/**
 * Created by pulasthi on 8/24/17.
 */
public class TextInputFormatter extends FileInputFormat {

  private static final Logger LOG = Logger.getLogger(TextInputFormatter.class.getName());

  private static final long serialVersionUID = 1L;

  /**
   * Code of \r, used to remove \r from a line when the line ends with \r\n.
   */
  private static final byte CARRIAGE_RETURN = (byte) '\r';

  /**
   * Code of \n, used to identify if \n is used as delimiter.
   */
  private static final byte NEW_LINE = (byte) '\n';

  /**
   * The name of the charset to use for decoding.
   */
  private String charsetName = "UTF-8";
}

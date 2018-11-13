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
package edu.iu.dsc.tws.data.api.splits;

import java.io.IOException;
import java.nio.charset.Charset;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;

public class TextInputSplit extends DelimitedSplit<String> {
  /**
   * Code of \r, used to remove \r from a line when the line ends with \r\n.
   */
  private static final byte CARRIAGE_RETURN = (byte) '\r';

  /**
   * Code of \n, used to identify if \n is used as delimiter.
   */
  private static final byte NEW_LINE = (byte) '\n';

  /**
   * Constructs a split with host information.
   *
   * @param num the number of this input split
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts the list of hosts containing the block, possibly <code>null</code>
   */
  public TextInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
  }

  @Override
  public String getCharsetName() {
    return charsetName;
  }

  @Override
  public void setCharsetName(String charsetName) {
    if (charsetName == null) {
      throw new IllegalArgumentException("Charset must not be null.");
    }
    this.charsetName = charsetName;
  }

  /**
   * The name of the charset to use for decoding.
   */
  private String charsetName = "UTF-8";

  @Override
  public void configure(Config parameters) {
    super.configure(parameters);

    if (charsetName == null || !Charset.isSupported(charsetName)) {
      throw new RuntimeException("Unsupported charset: " + charsetName);
    }
  }

  @Override
  public String readRecord(String reusable, byte[] bytes, int readOffset, int numBytes)
      throws IOException {
    //Check if \n is used as delimiter and the end of this line is a \r,
    // then remove \r from the line
    int curNumBytes = numBytes;
    if (this.getDelimiter() != null && this.getDelimiter().length == 1
        && this.getDelimiter()[0] == NEW_LINE && readOffset + curNumBytes >= 1
        && bytes[readOffset + curNumBytes - 1] == CARRIAGE_RETURN) {
      curNumBytes -= 1;
    }

    return new String(bytes, readOffset, numBytes, this.charsetName);
  }
}

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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Base class for inputs that are delimited
 */
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT>  {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(DelimitedInputFormat.class.getName());

  // The charset used to convert strings to bytes
  private String charsetName = "UTF-8";

  // Charset is not serializable
  private transient Charset charset;

  /**
   * The default read buffer size = 1MB.
   */
  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  /**
   * Indication that the number of samples has not been set by the configuration.
   */
  private static final int NUM_SAMPLES_UNDEFINED = -1;

  /**
   * The maximum number of line samples to be taken.
   */
  private static int DEFAULT_MAX_NUM_SAMPLES;

  /**
   * The minimum number of line samples to be taken.
   */
  private static int DEFAULT_MIN_NUM_SAMPLES;

  /**
   * The maximum size of a sample record before sampling is aborted. To catch cases where a wrong delimiter is given.
   */

  /**
   * The configuration key to set the record delimiter.
   */
  protected static final String RECORD_DELIMITER = "delimited-format.delimiter";


  private static int MAX_SAMPLE_LEN;



  private byte[] delimiter = new byte[] {'\n'};



  private String delimiterString = null;

  private int lineLengthLimit = Integer.MAX_VALUE;

  private int bufferSize = -1;

  private int numLineSamples = NUM_SAMPLES_UNDEFINED;


  public byte[] getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(byte[] delimiter) {
    if (delimiter == null) {
      throw new IllegalArgumentException("Delimiter must not be null");
    }
    this.delimiter = delimiter;
  }

  public String getDelimiterString() {
    return delimiterString;
  }

  public void setDelimiterString(String delimiterString) {
    if (delimiter == null) {
      throw new IllegalArgumentException("Delimiter must not be null");
    }
    this.delimiter = delimiterString.getBytes(getCharset());
    this.delimiterString = delimiterString;
  }


  /**
   * Get the character set used for the row delimiter.
   * @return the charset
   */
  public Charset getCharset() {
    if (this.charset == null) {
      this.charset = Charset.forName(charsetName);
    }
    return this.charset;
  }

  /**
   * This function parses the given byte array which represents a serialized record.
   * The function returns a valid record or throws an IOException.
   *
   * @param reuse An optionally reusable object.
   * @param bytes Binary data of serialized records.
   * @param offset The offset where to start to read the record data.
   * @param numBytes The number of bytes that can be read starting at the offset position.
   *
   * @return Returns the read record if it was successfully deserialized.
   * @throws IOException if the record could not be read.
   */
  public abstract OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) throws IOException;

  // --------------------------------------------------------------------------------------------
  //  Pre-flight: Configuration, Splits, Sampling
  // --------------------------------------------------------------------------------------------

  /**
   * Configures this input format by reading the path to the file from the configuration and the string that
   * defines the record delimiter.
   *
   * @param parameters The configuration object to read the parameters from.
   */
  @Override
  public void configure(Config parameters) {
    super.configure(parameters);

    // the if() clauses are to prevent the configure() method from
    // overwriting the values set by the setters

    if (Arrays.equals(delimiter, new byte[] {'\n'})) {
      String delimString = parameters.getStringValue(RECORD_DELIMITER, null);
      if (delimString != null) {
        setDelimiterString(delimString);
      }
    }

  }
}

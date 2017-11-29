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
import edu.iu.dsc.tws.data.fs.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;

/**
 * Base class for inputs that are delimited
 */
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> {

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
   * The maximum size of a sample record before sampling is aborted. To catch
   * cases where a wrong delimiter is given.
   */

  /**
   * The configuration key to set the record delimiter.
   */
  protected static final String RECORD_DELIMITER = "delimited-format.delimiter";

  private byte[] delimiter = new byte[]{'\n'};

  private String delimiterString = null;

  private int lineLengthLimit = Integer.MAX_VALUE;

  private int bufferSize = -1;

  // --------------------------------------------------------------------------------------------
  //  Variables for internal parsing.
  //  They are all transient, because we do not want them so be serialized
  // --------------------------------------------------------------------------------------------

  private transient byte[] readBuffer;

  private transient byte[] wrapBuffer;

  private transient int readPos;

  private transient int limit;

  private transient byte[] currBuffer;    // buffer in which current record byte sequence is found
  private transient int currOffset;      // offset in above buffer
  private transient int currLen;        // length of current byte sequence

  private transient boolean overLimit;

  private transient boolean end;

  private long offset = -1;


  public DelimitedInputFormat() {
    this(null, null);
  }

  protected DelimitedInputFormat(Path filePath, Config configuration) {
    super(filePath);
    if (configuration == null) {
      //TODO L2: Need to create global config
      //configuration = GlobalConfiguration.loadConfiguration();
    }
  }

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

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    if (bufferSize < 2) {
      throw new IllegalArgumentException("Buffer size must be at least 2.");
    }
    this.bufferSize = bufferSize;
  }

  public String getCharsetName() {
    return charsetName;
  }

  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }

  public int getLineLengthLimit() {
    return lineLengthLimit;
  }

  public void setLineLengthLimit(int lineLengthLimit) {
    this.lineLengthLimit = lineLengthLimit;
  }


  /**
   * Get the character set used for the row delimiter.
   *
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
   * @param readOffset The offset where to start to read the record data.
   * @param numBytes The number of bytes that can be read starting at the offset position.
   * @return Returns the read record if it was successfully deserialized.
   * @throws IOException if the record could not be read.
   */
  public abstract OT readRecord(OT reuse, byte[] bytes, int readOffset, int numBytes)
      throws IOException;

  @Override
  public OT nextRecord(OT record) throws IOException {
    if (readLine()) {
      return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
    } else {
      this.end = true;
      return null;
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pre-flight: Configuration, Splits, Sampling
  // --------------------------------------------------------------------------------------------

  /**
   * Configures this input format by reading the path to the file from the
   * configuration and the string that
   * defines the record delimiter.
   *
   * @param parameters The configuration object to read the parameters from.
   */
  @Override
  public void configure(Config parameters) {
    super.configure(parameters);

    // the if() clauses are to prevent the configure() method from
    // overwriting the values set by the setters

    if (Arrays.equals(delimiter, new byte[]{'\n'})) {
      String delimString = parameters.getStringValue(RECORD_DELIMITER, null);
      if (delimString != null) {
        setDelimiterString(delimString);
      }
    }

  }

  /**
   * Opens the given input split. This method opens the input stream to the specified file,
   * allocates read buffers
   * and positions the stream at the correct position, making sure that any partial
   * record at the beginning is skipped.
   *
   * @param split The input split to open.
   */
  public void open(FileInputSplit split) throws IOException {
    super.open(split);
    initBuffers();

    this.offset = splitStart;
    if (this.splitStart != 0) {
      this.stream.seek(offset);
      readLine();
      // if the first partial record already pushes the stream over
      // the limit of our split, then no record starts within this split
      if (this.overLimit) {
        this.end = true;
      }
    } else {
      fillBuffer(0);
    }
  }

  private void initBuffers() {
    this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

    if (this.bufferSize <= this.delimiter.length) {
      throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
    }

    if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
      this.readBuffer = new byte[this.bufferSize];
    }
    if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
      this.wrapBuffer = new byte[256];
    }

    this.readPos = 0;
    this.limit = 0;
    this.overLimit = false;
    this.end = false;
  }

  /**
   * Fills the read buffer with bytes read from the file starting from an offset.
   */
  private boolean fillBuffer(int fillOffset) throws IOException {
    int maxReadLength = this.readBuffer.length - fillOffset;
    // special case for reading the whole split.
    if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
      int read = this.stream.read(this.readBuffer, fillOffset, maxReadLength);
      if (read == -1) {
        this.stream.close();
        this.stream = null;
        return false;
      } else {
        this.readPos = fillOffset;
        this.limit = read;
        return true;
      }
    }

    // else ..
    int toRead;
    if (this.splitLength > 0) {
      // if we have more data, read that
      toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
    } else {
      // if we have exhausted our split, we need to complete the current record, or read one
      // more across the next split.
      // the reason is that the next split will skip over the beginning until it finds the first
      // delimiter, discarding it as an incomplete chunk of data that belongs to
      // the last record in the
      // previous split.
      toRead = maxReadLength;
      this.overLimit = true;
    }

    int read = this.stream.read(this.readBuffer, fillOffset, toRead);

    if (read == -1) {
      this.stream.close();
      this.stream = null;
      return false;
    } else {
      this.splitLength -= read;
      this.readPos = fillOffset; // position from where to start reading
      this.limit = read + fillOffset; // number of valid bytes in the read buffer
      return true;
    }
  }

  protected final boolean readLine() throws IOException {
    if (this.stream == null || this.overLimit) {
      return false;
    }

    int countInWrapBuffer = 0;

    // position of matching positions in the delimiter byte array
    int delimPos = 0;

    while (true) {
      if (this.readPos >= this.limit) {
        // readBuffer is completely consumed. Fill it again but keep partially read delimiter bytes.
        if (!fillBuffer(delimPos)) {
          int countInReadBuffer = delimPos;
          if (countInWrapBuffer + countInReadBuffer > 0) {
            // we have bytes left to emit
            if (countInReadBuffer > 0) {
              // we have bytes left in the readBuffer. Move them into the wrapBuffer
              if (this.wrapBuffer.length - countInWrapBuffer < countInReadBuffer) {
                // reallocate
                byte[] tmp = new byte[countInWrapBuffer + countInReadBuffer];
                System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
                this.wrapBuffer = tmp;
              }

              // copy readBuffer bytes to wrapBuffer
              System.arraycopy(this.readBuffer, 0, this.wrapBuffer,
                  countInWrapBuffer, countInReadBuffer);
              countInWrapBuffer += countInReadBuffer;
            }

            this.offset += countInWrapBuffer;
            setResult(this.wrapBuffer, 0, countInWrapBuffer);
            return true;
          } else {
            return false;
          }
        }
      }

      int startPos = this.readPos - delimPos;
      int count;

      // Search for next occurence of delimiter in read buffer.
      while (this.readPos < this.limit && delimPos < this.delimiter.length) {
        if ((this.readBuffer[this.readPos]) == this.delimiter[delimPos]) {
          // Found the expected delimiter character. Continue looking for the next
          // character of delimiter.
          delimPos++;
        } else {
          // Delimiter does not match.
          // We have to reset the read position to the character after
          // the first matching character
          //   and search for the whole delimiter again.
          readPos -= delimPos;
          delimPos = 0;
        }
        readPos++;
      }

      // check why we dropped out
      if (delimPos == this.delimiter.length) {
        // we found a delimiter
        int readBufferBytesRead = this.readPos - startPos;
        this.offset += countInWrapBuffer + readBufferBytesRead;
        count = readBufferBytesRead - this.delimiter.length;

        // copy to byte array
        if (countInWrapBuffer > 0) {
          // check wrap buffer size
          if (this.wrapBuffer.length < countInWrapBuffer + count) {
            final byte[] nb = new byte[countInWrapBuffer + count];
            System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
            this.wrapBuffer = nb;
          }
          if (count >= 0) {
            System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
          }
          setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
          return true;
        } else {
          setResult(this.readBuffer, startPos, count);
          return true;
        }
      } else {
        // we reached the end of the readBuffer
        count = this.limit - startPos;

        // check against the maximum record length
        if (((long) countInWrapBuffer) + count > this.lineLengthLimit) {
          throw new IOException("The record length exceeded the maximum record length ("
              + this.lineLengthLimit + ").");
        }

        // Compute number of bytes to move to wrapBuffer
        // Chars of partially read delimiter must remain in the readBuffer.
        // We might need to go back.
        int bytesToMove = count - delimPos;
        // ensure wrapBuffer is large enough
        if (this.wrapBuffer.length - countInWrapBuffer < bytesToMove) {
          // reallocate
          byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2,
              countInWrapBuffer + bytesToMove)];
          System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
          this.wrapBuffer = tmp;
        }

        // copy readBuffer to wrapBuffer (except delimiter chars)
        System.arraycopy(this.readBuffer, startPos, this.wrapBuffer,
            countInWrapBuffer, bytesToMove);
        countInWrapBuffer += bytesToMove;
        // move delimiter chars to the beginning of the readBuffer
        System.arraycopy(this.readBuffer, this.readPos - delimPos, this.readBuffer, 0, delimPos);

      }
    }
  }

  private void setResult(byte[] buffer, int resultOffset, int len) {
    this.currBuffer = buffer;
    this.currOffset = resultOffset;
    this.currLen = len;
  }

  /**
   * Closes the input by releasing all buffers and closing the file input stream.
   *
   * @throws IOException Thrown, if the closing of the file stream causes an I/O error.
   */
  @Override
  public void close() throws IOException {
    this.wrapBuffer = null;
    this.readBuffer = null;
    super.close();
  }

  /**
   * Checks whether the current split is at its end.
   *
   * @return True, if the split is at its end, false otherwise.
   */
  @Override
  public boolean reachedEnd() {
    return this.end;
  }
}

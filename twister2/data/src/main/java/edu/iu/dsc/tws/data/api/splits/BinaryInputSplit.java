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
import java.nio.ByteOrder;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.FileInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;

public class BinaryInputSplit extends FileInputSplit<byte[]> {
  /**
   * The configuration key to set the binary record length.
   */
  protected static final String RECORD_LENGTH = "binary-format.record-length";

  /**
   * The default value of the record length
   */
  protected static final int DEFAULT_RECORD_LENGTH = 8;

  /**
   * Endianess of the binary file, or the byte order
   */
  private ByteOrder endianess = ByteOrder.LITTLE_ENDIAN;

  /**
   * The default read buffer size = 1MB.
   */
  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  private int bufferSize = -1;

  /**
   * The length of a single record in the given binary file.
   */
  protected transient int recordLength;

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

  /**
   * Constructs a split with host information.
   *
   * @param num the number of this input split
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts the list of hosts containing the block, possibly <code>null</code>
   */
  public BinaryInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
  }

  public ByteOrder getEndianess() {
    return endianess;
  }

  public void setEndianess(ByteOrder order) {
    if (endianess == null) {
      throw new IllegalArgumentException("Endianess must not be null");
    }
    this.endianess = order;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int buffSize) {
    if (bufferSize < 2) {
      throw new IllegalArgumentException("Buffer size must be at least 2.");
    }
    this.bufferSize = buffSize;
  }

  public int getRecordLength() {
    return recordLength;
  }

  public void setRecordLength(int recordLen) {
    if (recordLen <= 0) {
      throw new IllegalArgumentException("RecordLength must be larger than 0");
    }
    this.recordLength = recordLen;
    if (this.bufferSize % recordLen != 0) {
      int bufferFactor = 1;
      if (this.bufferSize > 0) {
        bufferFactor = bufferSize / recordLen;
      } else {
        bufferFactor = DEFAULT_READ_BUFFER_SIZE / recordLen;
      }
      if (bufferFactor >= 1) {
        setBufferSize(recordLen * bufferFactor);
      } else {
        setBufferSize(recordLen * 8);
      }

    }
  }

  /**
   * Configures this input format by reading the path to the file from the configuration and setting
   * the record length
   *
   * @param parameters The configuration object to read the parameters from.
   */
  @Override
  public void configure(Config parameters) {
    super.configure(parameters);

    // the if() clauses are to prevent the configure() method from
    // overwriting the values set by the setters
    int recordLen = parameters.getIntegerValue(RECORD_LENGTH, -1);
    if (recordLen > 0) {
      setRecordLength(recordLen);
    }

  }

  /**
   * Opens the given input split. This method opens the input stream to the specified file,
   * allocates read buffers
   * and positions the stream at the correct position, making sure that any partial record at
   * the beginning is skipped.
   */
  public void open() throws IOException {
    open();
    initBuffers();
    //Check if we are starting at a new record and adjust as needed (only needed for binary files)
    long recordMod = this.splitStart % this.recordLength;
    if (recordMod != 0) {
      //We are not at the start of a record, we change the offset to take it to the start of the
      //next record
      this.offset = this.splitStart + this.recordLength - recordMod;
      //TODO: when debugging check if this shoould be >=
      if (this.offset > this.splitStart + this.splitLength) {
        this.end = true; // We do not have a record in this split
      }
    } else {
      this.offset = splitStart;
    }

    if (this.splitStart != 0) {
      this.stream.seek(offset);
    }
    fillBuffer(0);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return false;
  }

  @Override
  public byte[] nextRecord(byte[] record) throws IOException {
    if (checkAndBufferRecord()) {
      return readRecord(record, this.readBuffer, this.currOffset, this.currLen);
    } else {
      this.end = true;
      return null;
    }
  }

  /**
   * Reads a single record from the binary file
   */
  public byte[] readRecord(byte[] reusable, byte[] bytes, int readOffset, int numBytes)
      throws IOException {
    //If the reusable array is avilable use it
    //TODO L2: check for faster methods to perform this
    if (reusable != null && reusable.length == this.recordLength) {
      System.arraycopy(bytes, readOffset, reusable, 0, numBytes);
      return reusable;
    } else {
      //TODO L2:check if this has any memory leaks
      byte[] tmp = new byte[this.recordLength];
      System.arraycopy(bytes, readOffset, tmp, 0, numBytes);
      return tmp;
    }
  }



  private boolean checkAndBufferRecord() throws IOException {
    if (this.readPos < this.limit) {
      //The next record is already in the buffer
      this.currOffset = this.readPos;
      this.currLen = this.recordLength;
      this.readPos = this.readPos + this.recordLength;
      return true;
    } else {
      //Need to refill the buffer
      if (fillBuffer(0)) {
        this.currOffset = this.readPos;
        this.currLen = this.recordLength;
        this.readPos = this.readPos + this.recordLength;
        return true;
      } else {
        return false;
      }
    }
  }

  private void initBuffers() {
    this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

    if (this.bufferSize % this.recordLength != 0) {
      throw new IllegalArgumentException("Buffer size must be a multiple of the record length");
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
    if (this.splitLength == FileInputPartitioner.READ_WHOLE_SPLIT_FLAG) {
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
      return false; //Since the BIF does not break splits in the middle of records
      // there cannot be partial records
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
}

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
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileInputSplit;

/**
 * Input formatter class that reads binary files
 */
public class BinaryInputFormatter extends FileInputFormat<byte[]> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(BinaryInputFormatter.class.getName());

  /**
   * Endianess of the binary file, or the byte order
   */
  ByteOrder endianess = ByteOrder.BIG_ENDIAN;

  /**
   * The default read buffer size = 1MB.
   */
  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  /**
   * The input stream reading from the input file.
   */
  protected transient FSDataInputStream stream;

  /**
   * The length of a single record in the given binary file.
   */
  protected transient int recordLength;

  /**
   * The configuration key to set the binary record length.
   */
  protected static final String RECORD_LENGTH = "binary-format.record-length";

  private int bufferSize = -1;

  /**
   * The start of the split that this parallel instance must consume.
   */
  protected transient long splitStart;

  /**
   * The length of the split that this parallel instance must consume.
   */
  protected transient long splitLength;

  // --------------------------------------------------------------------------------------------
  //  Variables for internal parsing.
  //  They are all transient, because we do not want them so be serialized
  // --------------------------------------------------------------------------------------------

  private transient byte[] readBuffer;

  private transient byte[] wrapBuffer;

  private transient int readPos;

  private transient int limit;

  private transient byte[] currBuffer;		// buffer in which current record byte sequence is found
  private transient int currOffset;			// offset in above buffer
  private transient int currLen;				// length of current byte sequence

  private transient boolean overLimit;

  private transient boolean end;

  private long offset = -1;



  public ByteOrder getEndianess() {
    return endianess;
  }

  public void setEndianess(ByteOrder endianess) {
    if(endianess == null){
      throw new IllegalArgumentException("Endianess must not be null");
    }
    this.endianess = endianess;
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

  public int getRecordLength() {
    return recordLength;
  }

  public void setRecordLength(int recordLength) {
    if(recordLength <= 0){
      throw new IllegalArgumentException("RecordLength must be larger than 0");
    }
    this.recordLength = recordLength;
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
    int recordLength = parameters.getIntegerValue(RECORD_LENGTH, -1   );
    if (recordLength > 0) {
      setRecordLength(recordLength);
    }

  }

  /**
   * Opens the given input split. This method opens the input stream to the specified file, allocates read buffers
   * and positions the stream at the correct position, making sure that any partial record at the beginning is skipped.
   *
   * @param split The input split to open.
   */
  public void open(FileInputSplit split) throws IOException {
    super.open(split);
    initBuffers();
    //Check if we are starting at a new record and adjust as needed (only needed for binary files)
    long recordMod = this.splitStart % this.recordLength;
    if( recordMod != 0){
      //We are not at the start of a record, we change the offset to take it to the start of the
      //next record
      this.offset = this.splitStart + this.recordLength - recordMod;
      //TODO: when debugging check if this shoould be >=
      if(this.offset > this.splitStart + this.splitLength){
        this.end = true; // We do not have a record in this split
      }
    }else{
      this.offset = splitStart;
    }

    if (this.splitStart != 0) {
      this.stream.seek(offset);
    }
    fillBuffer(0);
  }

  private void initBuffers() {
    this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

    if (this.bufferSize %this.recordLength != 0) {
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
   * Reads a single record from the binary file
   * @return
   * @throws IOException
   */
  public byte[] readRecord(byte[] reusable, byte[] bytes, int offset, int numBytes) throws IOException {
    //If the reusable array is avilable use it
    //TODO L2: check for faster methods to perform this
    if(reusable != null && reusable.length == this.recordLength){
      System.arraycopy(bytes,offset,reusable,0,numBytes);
      return reusable;
    }else{
      //TODO L2:check if this has any memory leaks
      byte[] tmp = new byte[this.recordLength];
      System.arraycopy(bytes,offset,tmp,0,numBytes);
      return tmp;
    }
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

  private boolean checkAndBufferRecord() throws IOException {
    if(this.readPos < this.limit){
      //The next record is already in the buffer
      this.currOffset = this.readPos;
      this.currLen = this.recordLength;
      this.readPos = this.readPos + this.recordLength;
      return true;
    }else{
      //Need to refill the buffer
      if(fillBuffer(0)){
        this.currOffset = this.readPos;
        this.currLen = this.recordLength;
        this.readPos = this.readPos + this.recordLength;
        return true;
      }else{
        return false;
      }
    }
  }

  /**
   * Fills the read buffer with bytes read from the file starting from an offset.
   */
  private boolean fillBuffer(int offset) throws IOException {
    int maxReadLength = this.readBuffer.length - offset;
    // special case for reading the whole split.
    if (this.splitLength == FileInputFormat.READ_WHOLE_SPLIT_FLAG) {
      int read = this.stream.read(this.readBuffer, offset, maxReadLength);
      if (read == -1) {
        this.stream.close();
        this.stream = null;
        return false;
      } else {
        this.readPos = offset;
        this.limit = read;
        return true;
      }
    }

    // else ..
    int toRead;
    if (this.splitLength > 0) {
      // if we have more data, read that
      toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
    }
    else {
      // if we have exhausted our split, we need to complete the current record, or read one
      // more across the next split.
      // the reason is that the next split will skip over the beginning until it finds the first
      // delimiter, discarding it as an incomplete chunk of data that belongs to the last record in the
      // previous split.
      toRead = maxReadLength;
      this.overLimit = true;
    }

    int read = this.stream.read(this.readBuffer, offset, toRead);

    if (read == -1) {
      this.stream.close();
      this.stream = null;
      return false;
    } else {
      this.splitLength -= read;
      this.readPos = offset; // position from where to start reading
      this.limit = read + offset; // number of valid bytes in the read buffer
      return true;
    }
  }
}

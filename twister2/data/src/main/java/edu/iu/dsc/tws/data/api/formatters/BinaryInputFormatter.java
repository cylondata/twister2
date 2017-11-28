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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FileInputSplit;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

/**
 * Input formatter class that reads binary files
 */
public class BinaryInputFormatter extends FileInputFormat<byte[]> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(BinaryInputFormatter.class.getName());

  /**
   * Endianess of the binary file, or the byte order
   */
  private ByteOrder endianess = ByteOrder.LITTLE_ENDIAN;

  /**
   * The default read buffer size = 1MB.
   */
  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  /**
   * The length of a single record in the given binary file.
   */
  protected transient int recordLength;

  /**
   * The configuration key to set the binary record length.
   */
  protected static final String RECORD_LENGTH = "binary-format.record-length";

  /**
   * The default value of the record length
   */
  protected static final int DEFAULT_RECORD_LENGTH = 8;

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

  public BinaryInputFormatter(Path filePath) {
    super(filePath);
    setRecordLength(DEFAULT_RECORD_LENGTH);
  }

  public BinaryInputFormatter(Path filePath, int recordLen) {
    super(filePath);
    setRecordLength(recordLen);
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
   * Computes the input splits for the file. By default, one file block is one split. If more
   * splits are requested than blocks are available, then a split may be a fraction of a block and
   * splits may cross block boundaries.
   *
   * @param minNumSplits The minimum desired number of file splits.
   * @return The computed file splits.
   */
  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }
    //TODO L2: The current implementaion only handles a snigle binary file not a set of files
    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(curminNumSplits);
    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<FileStatus>();
    long totalLength = 0;

    final FileSystem fs = path.getFileSystem();
    final FileStatus pathFile = fs.getFileStatus(path);

    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      //TODO L3: implement test for unsplittable
      //testForUnsplittable(pathFile);

      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    //TODO L3: Handle if unsplittable
    //Splits will be made so that records are not broken into two splits
    //Odd records will be divided among the first splits so the max diff would be 1 record
    if (totalLength % this.recordLength != 0) {
      throw new IllegalStateException("The Binary file has a incomplete record");
    }
    long numberOfRecords = totalLength / this.recordLength;
    long minRecordsForSplit = Math.floorDiv(numberOfRecords, minNumSplits);
    long oddRecords = numberOfRecords % minNumSplits;

    //Generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();
      final long minSplitSize = minRecordsForSplit * this.recordLength;
      long currentSplitSize = minSplitSize;
      long halfSplit = currentSplitSize >>> 1;

      if (oddRecords > 0) {
        currentSplitSize = currentSplitSize + this.recordLength;
        oddRecords--;
      }

      if (len > 0) {
        // get the block locations and make sure they are in order with respect to their offset
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);

        long bytesUnassigned = len;
        long position = 0;

        int blockIndex = 0;
        while (bytesUnassigned >= currentSplitSize) {
          // get the block containing the majority of the data
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          // create a new split
          FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(),
              position, currentSplitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += currentSplitSize;
          bytesUnassigned -= currentSplitSize;
        }
      } else {
        throw new IllegalStateException("The binary file " + file.getPath() + " is Empty");
      }

    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  /**
   * Opens the given input split. This method opens the input stream to the specified file,
   * allocates read buffers
   * and positions the stream at the correct position, making sure that any partial record at
   * the beginning is skipped.
   *
   * @param split The input split to open.
   */
  public void open(FileInputSplit split) throws IOException {
    super.open(split);
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

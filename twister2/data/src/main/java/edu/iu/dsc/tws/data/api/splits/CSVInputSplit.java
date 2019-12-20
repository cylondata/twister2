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
import java.io.InputStreamReader;
import java.util.logging.Logger;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FSDataInputStream;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.formatters.FileInputPartitioner;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;

//public abstract class CSVInputSplit<OT> extends LocatableInputSplit<OT> {
public class CSVInputSplit extends FileInputSplit<byte[]> {

  private static final Logger LOG = Logger.getLogger(CSVInputSplit.class.getName());

  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_LINE_DELIMITER = "\n";
  public static final String DEFAULT_FIELD_DELIMITER = ",";

  private byte[] delimiter = new byte[]{'\n'};
  private String delimiterString = null;

  protected boolean lineDelimiterIsLinebreak = false;

  private final Path file;
  private Config config;
  protected FSDataInputStream stream;

  private long start;
  private long length;
  protected long splitStart;
  protected long splitLength;
  protected long openTimeout;

  protected int numSplits = -1;

  private transient byte[] wrapBuffer;
  private transient byte[] readBuffer;

  private transient boolean end;

  private transient int readPos;
  private transient int limit;
  protected transient int recordLength;
  private transient int currOffset;
  private transient int currLen;
  protected transient int commentCount;
  protected transient int invalidLineCount;

  private long offset = -1;
  private int bufferSize = -1;

  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  public CSVInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
    this.file = file;
    this.start = start;
    this.length = length;
  }

  public Path getPath() {
    return file;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public int hashCode() {
    return getSplitNumber() ^ (file == null ? 0 : file.hashCode());
  }

  public byte[] getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(byte[] delimiter) {
    if (delimiter == null) {
      throw new IllegalArgumentException("Delimiter must not be null");
    }
    this.delimiter = delimiter;
    this.delimiterString = null;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof CSVInputSplit && super.equals(obj)) {
      CSVInputSplit other = (CSVInputSplit) obj;
      return this.start == other.start
          && this.length == other.length
          && (this.file == null ? other.file == null : (other.file != null
          && this.file.equals(other.file)));
    } else {
      return false;
    }
  }

  public void configure(Config parameters) {
    this.config = parameters;

    int datasize = Integer.parseInt(String.valueOf(parameters.get(DataObjectConstants.DSIZE)));
    int recordLen = datasize * Double.BYTES;
    if (recordLen > 0) {
      setRecordLength(recordLen);
    }
  }

  public void setRecordLength(int recordLen) {
    if (recordLen <= 0) {
      throw new IllegalArgumentException("RecordLength must be larger than 0");
    }
    this.recordLength = recordLen;
    if (this.bufferSize % recordLen != 0) {
      int bufferFactor;
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

  public void setBufferSize(int buffSize) {
    if (buffSize < 2) {
      throw new IllegalArgumentException("Buffer size must be at least 2.");
    }
    this.bufferSize = buffSize;
  }

  @Override
  public void open() throws IOException {
    super.open();
    initBuffers();
    if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n') {
      this.lineDelimiterIsLinebreak = true;
    }

    this.commentCount = 0;
    this.invalidLineCount = 0;
    if (this.splitStart != 0) {
      this.stream.seek(this.offset);
    }

    CSVReader csvReader = new CSVReader(new InputStreamReader(this.stream));
    try {
      LOG.info("Read all csv values:" + csvReader.readAll());
    } catch (CsvException e) {
      e.printStackTrace();
    }
    fillBuffer(0);
  }

  @Override
  public void open(Config cfg) throws IOException {
    super.open(cfg);
    this.configure(cfg);
    initBuffers();
    this.commentCount = 0;
    this.invalidLineCount = 0;
    long recordMod = this.splitStart % this.recordLength;
    if (recordMod != 0) {
      this.offset = this.splitStart + this.recordLength - recordMod;
      if (this.offset > this.splitStart + this.splitLength) {
        this.end = true;
      }
    } else {
      this.offset = splitStart;
    }

    if (this.splitStart != 0) {
      this.stream.seek(offset);
    }
//    //TODO: Checking the integration of CSV
//    CSVParser csvParser = new CSVParserBuilder()
//        .withSeparator(',')
//        .withIgnoreQuotations(true)
//        .build();
//    LOG.info("stream reader:" + this.stream.getReader());
//    CSVReader csvReader = new CSVReaderBuilder(this.stream.getReader())
//        .withSkipLines(1) //if we put '1' it will skip the header
//        .withCSVParser(csvParser)
//        .build();
//    try {
//      LOG.info("csv reader contents:" + Arrays.toString(csvReader.readNext()));
//    } catch (CsvException e) {
//      e.printStackTrace();
//    }
    fillBuffer(0);
  }

  @Override
  public boolean reachedEnd() {
    return this.end;
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
      this.currOffset = this.readPos;
      this.currLen = this.recordLength;
      this.readPos = this.readPos + this.recordLength;
      return true;
    } else {
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
    this.end = false;
  }

  public byte[] readRecord(byte[] reusable, byte[] bytes, int readOffset, int numBytes) {
    if (reusable != null && reusable.length == this.recordLength) {
      System.arraycopy(bytes, readOffset, reusable, 0, numBytes);
      return reusable;
    } else {
      byte[] tmp = new byte[this.recordLength];
      System.arraycopy(bytes, readOffset, tmp, 0, numBytes);
      return tmp;
    }
  }

  private boolean fillBuffer(int fillOffset) throws IOException {
    int maxReadLength = this.readBuffer.length - fillOffset;
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

    int toRead;
    if (this.splitLength > 0) {
      toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
    } else {
      toRead = maxReadLength;
      return false;
    }

    int read = this.stream.read(this.readBuffer, fillOffset, toRead);
    if (read == -1) {
      this.stream.close();
      this.stream = null;
      return false;
    } else {
      this.splitLength -= read;
      this.readPos = fillOffset;
      this.limit = read + fillOffset;
      return true;
    }
  }
}

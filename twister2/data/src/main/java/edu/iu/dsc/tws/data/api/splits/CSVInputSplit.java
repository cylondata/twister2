package edu.iu.dsc.tws.data.api.splits;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.formatters.FileInputPartitioner;

public abstract class CSVInputSplit<OT> extends GenericCSVInputSplit {

  private static final Logger LOG = Logger.getLogger(CSVInputSplit.class.getName());

  // The charset used to convert strings to bytes
  private String charsetName = "UTF-8";

  public static final String DEFAULT_LINE_DELIMITER = "\n";

  public static final String DEFAULT_FIELD_DELIMITER = ",";

  // Charset is not serializable
  private transient Charset charset;

  /**
   * The default read buffer size = 1MB.
   */
  private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

  /**
   * The configuration key to set the record delimiter.
   */
  protected static final String RECORD_DELIMITER = "delimited-format.delimiter";

  private byte[] delimiter = new byte[]{'\n'};

  private String delimiterString = null;

  private int lineLengthLimit = Integer.MAX_VALUE;

  private int bufferSize = -1;

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

  private Config config;

  protected transient Object[] parsedValues;

  /**
   * Constructs a split with host information.
   *
   * @param num    the number of this input split
   * @param file   the file name
   * @param start  the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts  the list of hosts containing the block, possibly <code>null</code>
   */
  public CSVInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
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
   * Configures this input format by reading the path to the file from the
   * configuration and the string that
   * defines the record delimiter.
   *
   * @param parameters The configuration object to read the parameters from.
   */
  @Override
  public void configure(Config parameters) {
    super.configure(parameters);
    this.config = parameters;
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
   */
  public void open(Config cfg) throws IOException {
    super.open(cfg);
    initBuffers();
    this.offset = splitStart;
    this.parsedValues = new Object[2];
    for (int i = 0; i < 2; i++) {
      this.parsedValues[i] = createValue();
    }

    if (this.splitStart != 0) {
      this.stream.seek(offset);
      if (this.overLimit) {
        this.end = true;
      }
    } else {
      fillBuffer(0);
    }
  }

  private Double createValue() {
    return new Double(0.0);
  }

//  /**
//   * This function parses the given byte array which represents a serialized record.
//   * The function returns a valid record or throws an IOException.
//   *
//   * @param reuse      An optionally reusable object.
//   * @param bytes      Binary data of serialized records.
//   * @param readOffset The offset where to start to read the record data.
//   * @param numBytes   The number of bytes that can be read starting at the offset position.
//   * @return Returns the read record if it was successfully deserialized.
//   * @throws IOException if the record could not be read.
//   */
//  public abstract OT readRecord(OT reuse, byte[] bytes, int readOffset, int numBytes)
//      throws IOException;

//  @Override
//  public OT nextRecord(OT record) throws IOException {
//    LOG.info("I am reading the record:" + record.toString());
//    if (readLine()) {
//      return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
//    } else {
//      this.end = true;
//      return null;
//    }
//  }

  protected abstract OT fillRecord(OT reuse, Object[] parseValues);

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
      // if we have more data, read that
      toRead = this.splitLength > maxReadLength ? maxReadLength : (int) this.splitLength;
    } else {
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


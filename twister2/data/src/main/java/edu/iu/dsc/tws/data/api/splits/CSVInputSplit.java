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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.utils.PreConditions;

public class CSVInputSplit extends FileInputSplit<Object> {

  private static final Logger LOG = Logger.getLogger(CSVInputSplit.class.getName());

  public static final String DEFAULT_LINE_DELIMITER = "\n";

  private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];

  private static final boolean[] EMPTY_INCLUDED = new boolean[0];

  private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[]{','};

  // The charset used to convert strings to bytes
  private String charsetName = "UTF-8";

  private String delimiterString = null;

  private static final byte BACKSLASH = 92;

  // --------------------------------------------------------------------------------------------
  //  Variables for internal operation.
  //  They are all transient, because we do not want them so be serialized
  // --------------------------------------------------------------------------------------------
  private transient FieldParser<?>[] fieldParsers;

  private transient Charset charset;

  protected transient int commentCount;

  protected transient int invalidLineCount;

  protected transient Object[] parsedValues;

  private Class<?>[] fieldTypes = EMPTY_TYPES;

  protected boolean lineDelimiterIsLinebreak = false;

  protected boolean[] fieldIncluded = EMPTY_INCLUDED;

  private boolean lenient;

  private boolean skipFirstLineAsHeader;

  private boolean quotedStringParsing = false;

  private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;

  protected byte[] commentPrefix = null;

  private byte[] delimiter = new byte[]{'\n'};

  private byte quoteCharacter;

  private String fieldDelimString = null;

  private String commentPrefixString = null;

  public CSVInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
  }

  public CSVInputSplit(int num, Path file, String[] hosts) {
    super(num, file, hosts);
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return false;
  }

  @Override
  public Object nextRecord(Object reuse) throws IOException {
    Object returnRecord = null;
    do {
      returnRecord = nextRecord(reuse);
    } while (returnRecord == null && !reachedEnd());
    return returnRecord;
  }

  public Object readRecord(Object reuse, byte[] bytes, int offset, int numBytes)
      throws IOException {
    /*
     * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
     */
    // Found window's end line, so find carriage return before the newline
    if (this.lineDelimiterIsLinebreak && numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
      //reduce the number of bytes so that the Carriage return is not taken as data
      //numBytes--;
    }

    if (commentPrefix != null && commentPrefix.length <= numBytes) {
      //check record for comments
      boolean isComment = true;
      for (int i = 0; i < commentPrefix.length; i++) {
        if (commentPrefix[i] != bytes[offset + i]) {
          isComment = false;
          break;
        }
      }
      if (isComment) {
        this.commentCount++;
        return null;
      }
    }

    if (parseRecord(parsedValues, bytes, offset, numBytes)) {
      return fillRecord(reuse, parsedValues);
    } else {
      this.invalidLineCount++;
      return null;
    }
  }

  protected Object fillRecord(Object reuse, Object[] parsedvalues) {
    return null;
  }

  public void open(FileInputSplit split) throws IOException {
    //super.open(split);

    @SuppressWarnings("unchecked")
    FieldParser<Object>[] fieldparsers = (FieldParser<Object>[]) getFieldParsers();

    //throw exception if no field parsers are available
    if (fieldparsers.length == 0) {
      throw new IOException("CsvInputFormat.open(FileInputSplit split) - no field parsers "
          + "to parse input");
    }

    // create the value holders
    this.parsedValues = new Object[fieldparsers.length];
    for (int i = 0; i < fieldparsers.length; i++) {
      this.parsedValues[i] = fieldparsers[i].createValue();
    }

    // left to right evaluation makes access [0] okay
    // this marker is used to fasten up readRecord, so that it doesn't have to check each call
    // if the line ending is set to default
    if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n') {
      this.lineDelimiterIsLinebreak = true;
    }

    this.commentCount = 0;
    this.invalidLineCount = 0;
  }

  private Object getFieldParsers() {
    return null;
  }


  protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes)
      throws Twister2RuntimeException {

    boolean[] fieldincluded = this.fieldIncluded;

    int startPos = offset;
    final int limit = offset + numBytes;

    for (int field = 0, output = 0; field < fieldincluded.length; field++) {

      // check valid start position
      if (startPos > limit || (startPos == limit && field != fieldincluded.length - 1)) {
        if (lenient) {
          return false;
        } else {
          throw new Twister2RuntimeException("Row too short: " + new String(bytes, offset, numBytes,
              getCharset()));
        }
      }

      if (fieldincluded[field]) {
        // parse field
        @SuppressWarnings("unchecked")
        FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
        Object reuse = holders[output];
        startPos = parser.resetErrorStateAndParse(bytes, startPos, limit, this.fieldDelim, reuse);
        holders[output] = parser.getLastResult();

        // check parse result
        if (startPos < 0) {
          // no good
          if (lenient) {
            return false;
          } else {
            String lineAsString = new String(bytes, offset, numBytes, getCharset());
            throw new Twister2RuntimeException("Line could not be parsed: '" + lineAsString + "'\n"
                + "ParserError " + parser.getErrorState() + " \n"
                + "Expect field types: " + fieldTypesToString() + " \n"
                + "in file: " + currentSplit.getPath());
          }
        } else if (startPos == limit
            && field != fieldincluded.length - 1
            && !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
          // We are at the end of the record, but not all fields have been read
          // and the end is not a field delimiter indicating an empty last field.
          if (lenient) {
            return false;
          } else {
            throw new Twister2RuntimeException("Row too short: " + new String(bytes,
                offset, numBytes));
          }
        }
        output++;
      } else {
        // skip field
        startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
        if (startPos < 0) {
          if (!lenient) {
            String lineAsString = new String(bytes, offset, numBytes, getCharset());
            throw new Twister2RuntimeException("Line could not be parsed: '" + lineAsString + "'\n"
                + "Expect field types: " + fieldTypesToString() + " \n"
                + "in file: " + currentSplit.getPath());
          } else {
            return false;
          }
        } else if (startPos == limit
            && field != fieldincluded.length - 1
            && !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
          // We are at the end of the record, but not all fields have been read
          // and the end is not a field delimiter indicating an empty last field.
          if (lenient) {
            return false;
          } else {
            throw new Twister2RuntimeException("Row too short: " + new String(bytes,
                offset, numBytes));
          }
        }
      }
    }
    return true;
  }

  private String fieldTypesToString() {
    StringBuilder string = new StringBuilder();
    string.append(this.fieldTypes[0].toString());

    for (int i = 1; i < this.fieldTypes.length; i++) {
      string.append(", ").append(this.fieldTypes[i]);
    }
    return string.toString();
  }


  protected int skipFields(byte[] bytes, int startPos, int limit, byte[] delim) {

    int i = startPos;
    final int delimLimit = limit - delim.length + 1;

    if (quotedStringParsing && bytes[i] == quoteCharacter) {
      // quoted string parsing enabled and field is quoted
      // search for ending quote character, continue when it is escaped
      i++;
      while (i < limit && (bytes[i] != quoteCharacter || bytes[i - 1] == BACKSLASH)) {
        i++;
      }
      i++;

      if (i == limit) {
        // we are at the end of the record
        return limit;
      } else if (i < delimLimit && FieldParser.delimiterNext(bytes, i, delim)) {
        // we are not at the end, check if delimiter comes next
        return i + delim.length;
      } else {
        // delimiter did not follow end quote. Error...
        return -1;
      }
    } else {
      // field is not quoted
      while (i < delimLimit && !FieldParser.delimiterNext(bytes, i, delim)) {
        i++;
      }

      if (i >= delimLimit) {
        // no delimiter found. We are at the end of the record
        return limit;
      } else {
        // delimiter found.
        return i + delim.length;
      }
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
    this.delimiterString = null;
  }

  /**
   * Get the character set used for the row delimiter. This is also used by
   * subclasses to interpret field delimiters, comment strings, and for
   * configuring {@link FieldParser}s.
   *
   * @return the charset
   */
  @PublicEvolving
  public Charset getCharset() {
    if (this.charset == null) {
      this.charset = Charset.forName(charsetName);
    }
    return this.charset;
  }

  /**
   * Set the name of the character set used for the row delimiter. This is
   * also used by subclasses to interpret field delimiters, comment strings,
   * and for configuring {@link FieldParser}s.
   * <p>
   * These fields are interpreted when set. Changing the charset thereafter
   * may cause unexpected results.
   *
   * @param charset name of the charset
   */
  @PublicEvolving
  public void setCharset(String charset) {
    this.charsetName = PreConditions.checkNotNull(charset, null);
    this.charset = null;

    if (this.delimiterString != null) {
      this.delimiter = delimiterString.getBytes(getCharset());
    }
  }


  public void setDelimiter(char delimiter) {
    setDelimiter(String.valueOf(delimiter));
  }

  public void setDelimiter(String delimiter) {
    if (delimiter == null) {
      throw new IllegalArgumentException("Delimiter must not be null");
    }
    this.delimiter = delimiter.getBytes(getCharset());
    this.delimiterString = delimiter;
  }
}

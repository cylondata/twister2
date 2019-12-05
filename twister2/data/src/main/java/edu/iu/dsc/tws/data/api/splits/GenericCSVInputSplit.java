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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.data.Path;

public class GenericCSVInputSplit<OT> extends DelimitedInputSplit<OT> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(GenericCSVInputSplit.class.getName());

  private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];

  private static final boolean[] EMPTY_INCLUDED = new boolean[0];

  private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[]{','};

  private static final byte BACKSLASH = 92;

  //private transient FieldParser<?>[] fieldParsers;

  protected boolean lineDelimiterIsLinebreak = false;
  protected transient int commentCount;
  protected transient int invalidLineCount;

  private Class<?>[] fieldTypes = EMPTY_TYPES;

  protected boolean[] fieldIncluded = EMPTY_INCLUDED;

  private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;

  private String fieldDelimString = null;

  private boolean lenient;

  private boolean skipFirstLineAsHeader;

  private boolean quotedStringParsing = false;

  private byte quoteCharacter;

  protected byte[] commentPrefix = null;

  private String commentPrefixString = null;
  private int[] fieldParsers;

  /**
   * Constructs a split with host information.
   *
   * @param num    the number of this input split
   * @param file   the file name
   * @param start  the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts  the list of hosts containing the block, possibly <code>null</code>
   */
  public GenericCSVInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
  }

  @Override
  public Object readRecord(Object reuse, byte[] bytes, int readOffset, int numBytes)
      throws IOException {
    boolean[] included = this.fieldIncluded;

    int startPos = readOffset;
    final int limit = readOffset + numBytes;

    for (int field = 0, output = 0; field < included.length; field++) {

      // check valid start position
      if (startPos > limit || (startPos == limit && field != included.length - 1)) {
        if (lenient) {
          return false;
        } else {
//          throw new ParseException("Row too short: " + new String(
//          bytes, readOffset, numBytes, getCharset()));
        }
      }

      if (included[field]) {
        // parse field
        @SuppressWarnings("unchecked")
        FieldParser<Object> parser = null; // = (FieldParser<Object>) this.fieldParsers[output];
        Object[] holders = new Object[0];
        Object reuseobj = holders[output];
        startPos = parser.resetErrorStateAndParse(bytes, startPos, limit, this.fieldDelim,
            reuseobj);
        holders[output] = parser.getLastResult();

        // check parse result
        if (startPos < 0) {
          // no good
          if (lenient) {
            return false;
          } else {
            String lineAsString = new String(bytes, readOffset, numBytes, getCharset());
//            throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
//                + "ParserError " + parser.getErrorState() + " \n"
//                + "Expect field types: "+fieldTypesToString() + " \n"
//                + "in file: " + currentSplit.getPath());
          }
        } else if (startPos == limit
            && field != included.length - 1
            && !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
          // We are at the end of the record, but not all fields have been read
          // and the end is not a field delimiter indicating an empty last field.
          if (lenient) {
            return false;
          } else {
//            throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
          }
        }
        output++;
      } else {
        // skip field
        startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
        if (startPos < 0) {
          if (!lenient) {
            String lineAsString = new String(bytes, readOffset, numBytes, getCharset());
//            throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
//                + "Expect field types: "+fieldTypesToString()+" \n"
//                + "in file: " + currentSplit.getPath());
          } else {
            return false;
          }
        } else if (startPos == limit
            && field != included.length - 1
            && !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
          // We are at the end of the record, but not all fields have been read
          // and the end is not a field delimiter indicating an empty last field.
          if (lenient) {
            return false;
          } else {
//            throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
          }
        }
      }
    }
    return true;
  }

  private int skipFields(byte[] bytes, int startPos, int limit, byte[] fieldDelim) {
    return 1;
  }
}

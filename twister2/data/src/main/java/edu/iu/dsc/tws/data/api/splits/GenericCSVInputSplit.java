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

public abstract class GenericCSVInputSplit<OT> extends DelimitedInputSplit<OT> {

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

//  @Override
//  public void open(FileInputSplit split) throws IOException {
//    super.open(split);
//
//    // instantiate the parsers
//    FieldParser<?>[] parsers = new FieldParser<?>[fieldTypes.length];
//
//    for (int i = 0; i < fieldTypes.length; i++) {
//      if (fieldTypes[i] != null) {
//        Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
//        if (parserType == null) {
//          throw new RuntimeException("No parser available for type
//          '" + fieldTypes[i].getName() + "'.");
//        }
//
//        FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);
//
//        p.setCharset(getCharset());
//        if (this.quotedStringParsing) {
//          if (p instanceof StringParser) {
//            ((StringParser)p).enableQuotedStringParsing(this.quoteCharacter);
//          } else if (p instanceof StringValueParser) {
//            ((StringValueParser)p).enableQuotedStringParsing(this.quoteCharacter);
//          }
//        }
//
//        parsers[i] = p;
//      }
//    }
//    this.fieldParsers = parsers;
//
//    // skip the first line, if we are at the beginning of a file and have the option set
//    if (this.skipFirstLineAsHeader && this.splitStart == 0) {
//      readLine(); // read and ignore
//    }
//  }

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


  protected boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) {
    //throws ParseException {

    boolean[] fieldincluded = this.fieldIncluded;

    int startPos = offset;
    final int limit = offset + numBytes;

    for (int field = 0, output = 0; field < fieldincluded.length; field++) {

      // check valid start position
      if (startPos > limit || (startPos == limit && field != fieldincluded.length - 1)) {
        if (lenient) {
          return false;
        } else {
          //throw new ParseException("Row too short: " + new String(bytes, offset, numBytes,
          // getCharset()));
        }
      }

      if (fieldincluded[field]) {
        // parse field
        @SuppressWarnings("unchecked")
        //FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
        FieldParser<Object> parser = null;
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
            //throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
            //    + "ParserError " + parser.getErrorState() + " \n"
            //    + "Expect field types: "+fieldTypesToString() + " \n"
            //    + "in file: " + currentSplit.getPath());
          }
        } else if (startPos == limit
            && field != fieldincluded.length - 1
            && !FieldParser.endsWithDelimiter(bytes, startPos - 1, fieldDelim)) {
          // We are at the end of the record, but not all fields have been read
          // and the end is not a field delimiter indicating an empty last field.
          if (lenient) {
            return false;
          } else {
            //throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
          }
        }
        output++;
      } else {
        // skip field
        startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
        if (startPos < 0) {
          if (!lenient) {
            String lineAsString = new String(bytes, offset, numBytes, getCharset());
            //throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
            //    + "Expect field types: "+fieldTypesToString()+" \n"
            //   + "in file: " + currentSplit.getPath());
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
            //throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
          }
        }
      }
    }
    return true;
  }

  public abstract OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes)
      throws IOException;
}

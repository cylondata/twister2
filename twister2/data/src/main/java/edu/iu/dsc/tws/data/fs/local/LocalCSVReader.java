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
package edu.iu.dsc.tws.data.fs.local;

import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.utils.PreConditions;

public class LocalCSVReader {

  protected boolean[] includedMask;

  protected String lineDelimiter = CSVInputSplit.DEFAULT_LINE_DELIMITER;

  protected String fieldDelimiter = CSVInputSplit.DEFAULT_FIELD_DELIMITER;

  protected String commentPrefix = null;

  protected boolean parseQuotedStrings = false;

  protected char quoteCharacter = '"';

  protected boolean skipFirstLineAsHeader = false;

  protected boolean ignoreInvalidLines = false;

  private String charset = "UTF-8";

  public Path getFilePath() {
    return this.path;
  }

  private final Path path;

  public LocalCSVReader(Path filePath) {
    PreConditions.checkNotNull(filePath, "File is not null");
    this.path = filePath;
  }

  public LocalCSVReader lineDelimiter(String delimiter) {
    if (delimiter == null || delimiter.length() == 0) {
      throw new IllegalArgumentException("The delimiter must not be null or an empty string");
    }

    this.lineDelimiter = delimiter;
    return this;
  }

  public LocalCSVReader fieldDelimiter(char delimiter) {
    this.fieldDelimiter = String.valueOf(delimiter);
    return this;
  }

  public LocalCSVReader fieldDelimiter(String delimiter) {
    this.fieldDelimiter = delimiter;
    return this;
  }

  public LocalCSVReader parseQuotedStrings(char quotecharacter) {
    this.parseQuotedStrings = true;
    this.quoteCharacter = quotecharacter;
    return this;
  }

  public LocalCSVReader ignoreComments(String commentprefix) {
    if (commentPrefix == null || commentprefix.length() == 0) {
      throw new IllegalArgumentException("The comment prefix must not be null or an empty string");
    }

    this.commentPrefix = commentprefix;
    return this;
  }

  public String getCharset() {
    return this.charset;
  }

  public void setCharset(String charset) {
    this.charset = PreConditions.checkNotNull(charset, "Not null");
  }

  public LocalCSVReader ignoreFirstLine() {
    skipFirstLineAsHeader = true;
    return this;
  }

  public LocalCSVReader ignoreInvalidLines() {
    ignoreInvalidLines = true;
    return this;
  }

//  private void configureInputSplit(CSVInputSplit<?> inputSplit) {
//    inputSplit.setBufferSize(2000);
//    inputSplit.setCharsetName(this.charset);
//    //inputSplit.setDelimiter(this.lineDelimiter);
//    //inputSplit.setSkipFirstLineAsHeader(skipFirstLineAsHeader);
//    //inputSplit.setLenient(ignoreInvalidLines);
//  }
}

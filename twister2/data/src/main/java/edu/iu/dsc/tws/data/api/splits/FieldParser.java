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

public class FieldParser<T> {

  public int resetErrorStateAndParse(byte[] bytes, int startPos, int limit, byte[] fieldDelim,
                                     Object reuse) {
    return 1;
  }

  public Object getLastResult() {
    return null;
  }

  public static final boolean delimiterNext(byte[] bytes, int startPos, byte[] delim) {

    for (int pos = 0; pos < delim.length; pos++) {
      // check each position
      if (delim[pos] != bytes[startPos + pos]) {
        return false;
      }
    }
    return true;

  }

  public static final boolean endsWithDelimiter(byte[] bytes, int endPos, byte[] delim) {
    if (endPos < delim.length - 1) {
      return false;
    }
    for (int pos = 0; pos < delim.length; ++pos) {
      if (delim[pos] != bytes[endPos - delim.length + 1 + pos]) {
        return false;
      }
    }
    return true;
  }

  public T createValue() {
    return null;
  }

  public T getErrorState() {
    return null;
  }
}

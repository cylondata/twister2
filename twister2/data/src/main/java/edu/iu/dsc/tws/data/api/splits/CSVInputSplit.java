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

import edu.iu.dsc.tws.data.fs.Path;

public class CSVInputSplit extends DelimitedInputSplit<String> {
  /**
   * Constructs a split with host information.
   *
   * @param num the number of this input split
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts the list of hosts containing the block, possibly <code>null</code>
   */
  public CSVInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, file, start, length, hosts);
  }

  @Override
  public String readRecord(String reuse, byte[] bytes,
                           int readOffset, int numBytes) throws IOException {

    return null;
  }
}

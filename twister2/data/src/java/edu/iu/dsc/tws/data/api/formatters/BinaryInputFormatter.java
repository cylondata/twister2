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

import java.nio.ByteOrder;
import java.util.logging.Logger;

import edu.iu.dsc.tws.data.fs.FSDataInputStream;

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
  protected transient long recordLength;
  
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
}

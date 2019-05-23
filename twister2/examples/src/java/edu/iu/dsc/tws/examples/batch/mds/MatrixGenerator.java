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
package edu.iu.dsc.tws.examples.batch.mds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import org.apache.commons.lang3.RandomStringUtils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FSDataOutputStream;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

public class MatrixGenerator {

  private static final Logger LOG = Logger.getLogger(MatrixGenerator.class.getName());

  private Config config;
  private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

  private static int dataTypeSize = Short.BYTES;
  private int workerId;

  public MatrixGenerator(Config cfg, int workerid) {
    this.config = cfg;
    this.workerId = workerid;
  }

  /**
   * To generate the matrix for MDS application
   * @param numberOfRows
   * @param dimensions
   * @param directory
   * @param byteType
   */
  public void generate(int numberOfRows, int dimensions, String directory, String byteType) {

    endianness = "big".equals(byteType) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

    short[] input = new short[numberOfRows * dimensions];
    for (int i = 0; i < numberOfRows * dimensions; i++) {
      double temp = Math.random() * Short.MAX_VALUE;
      input[i] = (short) temp;
    }
    try {
      ByteBuffer byteBuffer = ByteBuffer.allocate(numberOfRows * dimensions * 2);
      if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
      } else {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      byteBuffer.clear();
      ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
      shortOutputBuffer.put(input);

      Path path = new Path(directory);
      FileSystem fs = FileSystem.get(path.toUri(), config);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      FSDataOutputStream outputStream = fs.create(new Path(directory, generateRandom(10) + ".bin"));
      FileChannel out = outputStream.getChannel();
      out.write(byteBuffer);
      outputStream.sync();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("IOException Occured" + e.getMessage());
    }
  }

  private static String generateRandom(int length) {
    boolean useLetters = true;
    boolean useNumbers = false;
    return RandomStringUtils.random(length, useLetters, useNumbers);
  }
}

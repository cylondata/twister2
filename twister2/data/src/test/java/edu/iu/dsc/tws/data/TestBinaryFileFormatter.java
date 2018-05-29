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
package edu.iu.dsc.tws.data;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.api.formatters.BinaryInputFormatter;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

/**
 * Test Class for Binary formatter
 */
public class TestBinaryFileFormatter {
  public static void main(String[] args) {
    Config.Builder builder = new Config.Builder();
    builder.put("input.file.path", "/home/pulasthi/git/twister2/twister2/"
        + "data/src/test/resources/2000.bin");
    Config txtFileConf = builder.build();
    Path path = new Path("/home/pulasthi/git/twister2/twister2/data/src/test/resources/2000.bin");
    InputFormat binaryInputFormatter = new BinaryInputFormatter(path, 2000 * Short.BYTES);
    binaryInputFormatter.configure(txtFileConf);
    int minSplits = 8;
    double expectedSum = 1.97973979E8;
    double newSum = 0.0;
    int count = 0;
    Buffer buffer = null;

    try {
      InputSplit[] inputSplits = binaryInputFormatter.createInputSplits(minSplits);
      InputSplitAssigner inputSplitAssigner = binaryInputFormatter.
          getInputSplitAssigner(inputSplits);
      InputSplit currentSplit;
      byte[] line = new byte[4000];
      ByteBuffer byteBuffer = ByteBuffer.allocate(4000);
      byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
      while ((currentSplit = inputSplitAssigner.getNextInputSplit(null, 0)) != null) {
        binaryInputFormatter.open(currentSplit);
        while (binaryInputFormatter.nextRecord(line) != null) {
          byteBuffer.clear();
          byteBuffer.put(line);
          byteBuffer.flip();
          buffer = byteBuffer.asShortBuffer();
          short[] shortArray = new short[2000];
          ((ShortBuffer) buffer).get(shortArray);
          for (short i : shortArray) {
            newSum += i;
            count++;
          }
        }
      }

      System.out.println(newSum);
      System.out.println(count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

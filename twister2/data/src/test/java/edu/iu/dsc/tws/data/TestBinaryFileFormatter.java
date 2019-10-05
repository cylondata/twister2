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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.BinaryInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

/**
 * Test Class for Binary formatter
 */
public final class TestBinaryFileFormatter {

  private static final Logger LOG = Logger.getLogger(TestBinaryFileFormatter.class.getName());

  private TestBinaryFileFormatter() {

  }

  public static void main(String[] args) {

    Config.Builder builder = new Config.Builder();
    builder.put("input.file.path", "/tmp/2000.bin");
    builder.put("RECORD_LENGTH", 1000 * Short.BYTES);
    Config txtFileConf = builder.build();

    Path path = new Path("/tmp/2000.bin");
    InputPartitioner binaryInputPartitioner = new BinaryInputPartitioner(
        path, 1000 * Short.BYTES);
    binaryInputPartitioner.configure(txtFileConf);

    int count = 0;
    int minSplits = 4;

    double expectedSum = 1.6375350724E1;
    double newSum = 0.0;
    Buffer buffer;
    try {
      InputSplit[] inputSplits = binaryInputPartitioner.createInputSplits(minSplits);
      InputSplitAssigner inputSplitAssigner = binaryInputPartitioner.
          getInputSplitAssigner(inputSplits);
      InputSplit currentSplit;

      byte[] line = new byte[2000];
      ByteBuffer byteBuffer = ByteBuffer.allocate(2000);
      byteBuffer.order(ByteOrder.BIG_ENDIAN);
      while ((currentSplit = inputSplitAssigner.getNextInputSplit("localhost", 0))
          != null) {
        currentSplit.open(txtFileConf);
        while (currentSplit.nextRecord(line) != null) {
          byteBuffer.clear();
          byteBuffer.put(line);
          byteBuffer.flip();
          buffer = byteBuffer.asShortBuffer();
          short[] shortArray = new short[1000];
          ((ShortBuffer) buffer).get(shortArray);
          for (short i : shortArray) {
            newSum += i;
            count++;
          }
        }
      }
      LOG.info("Sum and count values are:" + newSum + "\t" + count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

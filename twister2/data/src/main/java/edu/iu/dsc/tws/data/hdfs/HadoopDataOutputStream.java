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
package edu.iu.dsc.tws.data.hdfs;

import java.io.IOException;

import edu.iu.dsc.tws.data.fs.FSDataOutputStream;

public final class HadoopDataOutputStream extends FSDataOutputStream {

  private final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream;

  public HadoopDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream1) {
    if (fsDataOutputStream1 == null) {
      throw new NullPointerException();
    }
    this.fsDataOutputStream = fsDataOutputStream1;
  }

  @Override
  public void write(int b) throws IOException {
    fsDataOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    fsDataOutputStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    fsDataOutputStream.close();
  }

  @Override
  public long getPos() throws IOException {
    return fsDataOutputStream.getPos();
  }

  @Override
  public void flush() throws IOException {
    fsDataOutputStream.hflush();
  }

  @Override
  public void sync() throws IOException {
    fsDataOutputStream.hsync();
  }

  public org.apache.hadoop.fs.FSDataOutputStream getHadoopOutputStream() {
    return fsDataOutputStream;
  }

}

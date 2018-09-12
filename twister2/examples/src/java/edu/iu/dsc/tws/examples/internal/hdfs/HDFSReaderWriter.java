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
package edu.iu.dsc.tws.examples.internal.hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;

public class HDFSReaderWriter {

  private static final Logger LOG = Logger.getLogger(HDFSReaderWriter.class.getName());

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private HdfsUtils hdfsUtils;
  private Config config;
  private String fileName;

  protected HDFSReaderWriter(Config cfg, String fName) {
    this.config = cfg;
    this.fileName = fName;
    hdfsUtils = new HdfsUtils(config, fileName);
  }

  public void readInputFromHDFS() {

    HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
    Path path = hdfsUtils.getPath();
    BufferedReader br = null;

    try {
      if (hadoopFileSystem.exists(path)) {
        br = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(path)));
        String line = "";
        while ((line = br.readLine()) != null) {
          LOG.info("File Contents Are:" + line);
        }
        br.close();
      } else {
        throw new FileNotFoundException("File Not Found In HDFS");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void writeOutputToHDFS() {

    HadoopFileSystem hadoopFileSystem = hdfsUtils.createHDFSFileSystem();
    Path path = hdfsUtils.getPath();
    HadoopDataOutputStream hadoopDataOutputStream = null;

    try {
      if (!hadoopFileSystem.exists(path)) {
        hadoopDataOutputStream = hadoopFileSystem.create(path);
        for (int i = 0; i < 20; i++) {
          hadoopDataOutputStream.write(
                  "Hello, writing to Data Output Stream\n".getBytes(DEFAULT_CHARSET));
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      try {
        if (hadoopFileSystem != null) {
          hadoopFileSystem.close();
          hadoopDataOutputStream.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}

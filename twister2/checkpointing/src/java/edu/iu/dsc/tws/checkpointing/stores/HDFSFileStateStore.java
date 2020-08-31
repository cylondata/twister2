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
package edu.iu.dsc.tws.checkpointing.stores;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.IOUtils;

import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.FileSystemContext;
import edu.iu.dsc.tws.api.data.FSDataInputStream;
import edu.iu.dsc.tws.api.data.FSDataOutputStream;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class HDFSFileStateStore implements StateStore {

  private static final String HDFS_PROTO = "hdfs://";

  private String parentPath;
  private FileSystem hdfs;

  @Override
  public void init(Config config, String... path) {
    String finalPath =
        HDFS_PROTO + String.join(File.separator, FileSystemContext.persistentStorageRoot(config),
            String.join(File.separator, path));
    this.parentPath = finalPath;
    try {
      this.hdfs = FileSystemUtils.get(URI.create(finalPath), config);
      this.hdfs.mkdirs(new Path(finalPath));
    } catch (IOException e) {
      throw new RuntimeException("Couldn't initial HDFS Store. "
          + "Failed to create the root directory, " + this.parentPath, e);
    }
  }

  private Path getPathForKey(String key) {
    return new Path(this.parentPath, key);
  }

  @Override
  public void put(String key, byte[] data) throws IOException {
    Path pathForKey = this.getPathForKey(key);
    FSDataOutputStream hadoopDataOutputStream = this.hdfs.create(pathForKey);
    IOUtils.copyBytes(new ByteArrayInputStream(data),
        hadoopDataOutputStream, data.length, true);
  }

  @Override
  public byte[] get(String key) throws IOException {
    if (!this.contains(key)) {
      return null;
    }
    FSDataInputStream hadoopDataInputStream = this.hdfs.open(this.getPathForKey(key));
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    IOUtils.copyBytes(hadoopDataInputStream, outStream, 1024, true);
    return outStream.toByteArray();
  }

  @Override
  public boolean contains(String key) throws IOException {
    return this.hdfs.exists(this.getPathForKey(key));
  }
}

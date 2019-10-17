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
package edu.iu.dsc.tws.dataset.partition;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class HDFSBackedCollectionPartition<T> extends BufferedCollectionPartition<T> {

  private static final String CONFIG_HDFS_ROOT = "twister2.data.hdfs.root";
  private static final String HDFS_PROTO = "hdfs://";

  public HDFSBackedCollectionPartition(int maxFramesInMemory, MessageType dataType,
                                       int bufferedBytes, Config config, String reference) {
    super(maxFramesInMemory, dataType, bufferedBytes, config, reference);
  }

  public HDFSBackedCollectionPartition(int maxFramesInMemory, Config config) {
    super(maxFramesInMemory, config);
  }

  public HDFSBackedCollectionPartition(int maxFramesInMemory, MessageType dataType, Config config) {
    super(maxFramesInMemory, dataType, config);
  }

  public HDFSBackedCollectionPartition(MessageType dataType, int bufferedBytes, Config config) {
    super(dataType, bufferedBytes, config);
  }

  public HDFSBackedCollectionPartition(MessageType dataType, int bufferedBytes,
                                       Config config, String reference) {
    super(dataType, bufferedBytes, config, reference);
  }

  protected String getRootPathStr(Config config) {
    return HDFS_PROTO + String.join(File.separator,
        config.getStringValue(CONFIG_HDFS_ROOT), String.join(File.separator,
            this.getReference()));
  }

  @Override
  protected FileSystem getFileSystem(Config config) {
    try {
      return FileSystemUtils.get(URI.create(getRootPathStr(config)), config);
    } catch (IOException e) {
      throw new Twister2RuntimeException("Error in connecting to file system", e);
    }
  }

  @Override
  protected Path getRootPath(Config config) {
    return new Path(getRootPathStr(config));
  }
}

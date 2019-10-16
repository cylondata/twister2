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
import java.util.UUID;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class DiskBackedCollectionPartition<T> extends BufferedCollectionPartition<T> {

  private static final String CONFIG_FS_ROOT = "twister2.data.fs.root";
  private static final String FS_PROTO = "file://";
  private String rootPath;

  public DiskBackedCollectionPartition(int maxFramesInMemory, MessageType dataType,
                                       int bufferedBytes, Config config) {
    super(maxFramesInMemory, dataType, bufferedBytes, config);
  }

  public DiskBackedCollectionPartition(int maxFramesInMemory, Config config) {
    super(maxFramesInMemory, config);
  }

  public DiskBackedCollectionPartition(int maxFramesInMemory, MessageType dataType, Config config) {
    super(maxFramesInMemory, dataType, config);
  }

  public DiskBackedCollectionPartition(MessageType dataType, int bufferedBytes, Config config) {
    super(dataType, bufferedBytes, config);
  }

  protected String getRootPathStr(Config config) {
    if (this.rootPath == null) {
      this.rootPath = FS_PROTO + String.join(File.separator,
          config.getStringValue(CONFIG_FS_ROOT), String.join(File.separator,
              UUID.randomUUID().toString()));
    }
    return rootPath;
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

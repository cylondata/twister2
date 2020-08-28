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
import edu.iu.dsc.tws.api.data.FileSystemContext;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

@SuppressWarnings("rawtypes")
public class DiskBackedCollectionPartition<T> extends BufferedCollectionPartition<T> {

  private static final String FS_PROTO = "file://";
  public static final String CONFIG = "local";

  public DiskBackedCollectionPartition(long maxFramesInMemory, MessageType dataType,
                                       long bufferedBytes, Config config, String reference) {
    super(maxFramesInMemory, dataType, bufferedBytes, config, reference);
  }

  public DiskBackedCollectionPartition(long maxFramesInMemory, Config config) {
    super(maxFramesInMemory, config);
  }

  public DiskBackedCollectionPartition(long maxFramesInMemory, Config config,
                                       String reference) {
    super(maxFramesInMemory, config, reference);
  }

  public DiskBackedCollectionPartition(long maxFramesInMemory, MessageType dataType,
                                       Config config) {
    super(maxFramesInMemory, dataType, config);
  }

  public DiskBackedCollectionPartition(MessageType dataType, long bufferedBytes, Config config) {
    super(dataType, bufferedBytes, config);
  }

  public DiskBackedCollectionPartition(MessageType dataType, long bufferedBytes,
                                       Config config, String reference) {
    super(dataType, bufferedBytes, config, reference);
  }

  protected String getRootPathStr(Config config) {
    return FS_PROTO + String.join(File.separator, FileSystemContext.volatileStorageRoot(config),
        "tsetdata", this.getReference());
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

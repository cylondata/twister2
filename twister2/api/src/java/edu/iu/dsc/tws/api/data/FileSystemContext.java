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
package edu.iu.dsc.tws.api.data;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;

public class FileSystemContext {

  public static final String PERSISTENT_STORAGE_TYPE = "twister2.persistent.storage.type";

  public static final String PERSISTENT_STORAGE_ROOT = "twister2.persistent.storage.root";

  protected FileSystemContext() { }

  public static String persistentStorageType(Config config) {
    return config.getStringValue(PERSISTENT_STORAGE_TYPE, "local");
  }

  public static String persistentStorageRoot(Config config) {
    String rootPath = config.getStringValue(PERSISTENT_STORAGE_ROOT);
    if (rootPath == null) {
      return null;
    }

    //TODO: we can replace slash with filesystem specific separator
    //      however, slash should work with NFS and HDFS
    return rootPath + "/" + Context.jobId(config);
  }

  public static String checkpointingStoreClass(Config config) {
    String type = persistentStorageType(config);
    if (type == null) {
      return null;
    }

    switch (type) {
      case "hdfs":
        return "edu.iu.dsc.tws.checkpointing.stores.HDFSFileStateStore";

      case "nfs":
        return "edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore";

      case "local":
        return "edu.iu.dsc.tws.checkpointing.stores.LocalFileStateStore";

      default:
        return null;
    }
  }



}

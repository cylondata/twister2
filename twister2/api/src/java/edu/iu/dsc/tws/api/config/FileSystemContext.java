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
package edu.iu.dsc.tws.api.config;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class FileSystemContext {

  public static final String PERSISTENT_STORAGE_TYPE = "twister2.persistent.storage.type";
  public static final String PERSISTENT_STORAGE_TYPE_DEFAULT = "mounted";
  public static final String PERSISTENT_STORAGE_ROOT = "twister2.persistent.storage.root";

  public static final String VOLATILE_STORAGE_ROOT_DEFAULT = "/tmp/twister2/volatile";
  public static final String VOLATILE_STORAGE_ROOT = "twister2.volatile.storage.root";

  public static final String TSET_STORAGE_TYPE_DEFAULT = "mounted";
  public static final String TSET_STORAGE_TYPE = "twister2.tset.storage.type";

  protected FileSystemContext() { }

  public static String persistentStorageType(Config config) {
    return config.getStringValue(PERSISTENT_STORAGE_TYPE, PERSISTENT_STORAGE_TYPE_DEFAULT);
  }

  public static String persistentStorageRoot(Config config) {
    if (Context.isKubernetesCluster(config)
        && "mounted".equalsIgnoreCase(persistentStorageType(config))) {
      // this is from: KubernetesConstants.PERSISTENT_VOLUME_MOUNT
      // but we can not reference that from this location, because of its package
      return "/persistent";
    }

    String rootPath = config.getStringValue(PERSISTENT_STORAGE_ROOT);
    if (rootPath == null) {
      throw new Twister2RuntimeException(PERSISTENT_STORAGE_ROOT + " is not specified in configs");
    }

    return rootPath + File.separator + Context.jobId(config);
  }

  public static String volatileStorageRoot(Config config) {
    if (Context.isKubernetesCluster(config)) {
      // this is from: KubernetesConstants.POD_VOLATILE_VOLUME
      // but we can not reference that from this location, because of its package
      return "/twister2-volatile";
    }

    List<String> rootPaths = config.getListValue(VOLATILE_STORAGE_ROOT);
    String rootPath;
    if (rootPaths == null || rootPaths.size() == 0) {
      rootPath = VOLATILE_STORAGE_ROOT_DEFAULT;
    } else {
      rootPath = rootPaths.get(0);
    }

    return rootPath + File.separator + Context.jobId(config);
  }

  public static List<String> volatileStorageRoots(Config config) {
    if (Context.isKubernetesCluster(config)) {
      // this is from: KubernetesConstants.POD_VOLATILE_VOLUME
      // but we can not reference that from this location, because of its package
      return Stream.of("/twister2-volatile").collect(Collectors.toList());
    }

    List<String> rootPaths = config.getListValue(VOLATILE_STORAGE_ROOT);
    if (rootPaths == null || rootPaths.size() == 0) {
      rootPaths = new LinkedList<>();
      rootPaths.add(VOLATILE_STORAGE_ROOT_DEFAULT);
    }

    return rootPaths.stream()
        .map(path -> path + File.separator + Context.jobId(config))
        .collect(Collectors.toList());
  }

  public static String tsetStorageType(Config config) {
    return config.getStringValue(TSET_STORAGE_TYPE, TSET_STORAGE_TYPE_DEFAULT);
  }

}

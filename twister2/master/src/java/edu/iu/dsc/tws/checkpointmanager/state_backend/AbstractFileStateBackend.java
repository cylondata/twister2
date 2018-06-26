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
package edu.iu.dsc.tws.checkpointmanager.state_backend;

import java.net.URI;


import edu.iu.dsc.tws.data.fs.Path;

public abstract class AbstractFileStateBackend extends AbstractStateBackend {
  private static final long serialVersionUID = 1L;


  private final Path baseCheckpointPath;

  protected AbstractFileStateBackend(Path baseCheckpointPath) {
    this.baseCheckpointPath = baseCheckpointPath == null ? null : validatePath(baseCheckpointPath);
  }

  public AbstractFileStateBackend(URI baseCheckpointPath) {
    this((baseCheckpointPath == null) ? null : new Path(baseCheckpointPath));

  }


  public Path getCheckpointPath() {
    return baseCheckpointPath;
  }


  private static Path validatePath(Path path) {
    final URI uri = path.toUri();
    final String scheme = uri.getScheme();
    final String pathPart = uri.getPath();

    if (scheme == null) {
      throw new IllegalArgumentException("The scheme (hdfs://, file://, etc) is null. "
          + "Please specify the file system scheme explicitly in the URI.");
    }
    if (pathPart == null) {

      throw new IllegalArgumentException("The path to store the checkpoint data in is null. "
          + "Please specify a directory path for the checkpoint data.");
    }
    if (pathPart.length() == 0 || "/".equals(pathPart)) {

      throw new IllegalArgumentException("Cannot use the root directory for checkpoints.");
    }

    return path;
  }
}

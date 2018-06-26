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
package edu.iu.dsc.tws.data.fs.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FSDataOutputStream;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.utils.OperatingSystem;

/**
 * Represents a local file system.
 */
public class LocalFileSystem extends FileSystem {

  private static final Logger LOG = Logger.getLogger(LocalFileSystem.class.getName());

  /**
   * The URI representing the local file system.
   */
  private static URI uri = OperatingSystem.isWindows() ? URI.create("file:/")
      : URI.create("file:///");

  /**
   * Path pointing to the current working directory.
   * Because Paths are not immutable, we cannot cache the proper path here
   */
  private final String workingDir;

  /**
   * Path pointing to the current working directory.
   * Because Paths are not immutable, we cannot cache the proper path here
   */
  private final String homeDir;

  /**
   * The host name of this machine
   */
  private final String hostName;

  /**
   * Constructs a new <code>LocalFileSystem</code> object.
   */
  public LocalFileSystem() {
    this.workingDir = new Path(System.getProperty("user.dir")).makeQualified(this).toString();
    this.homeDir = new Path(System.getProperty("user.home")).toString();

    String tmp = "unknownHost";
    try {
      tmp = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Could not resolve local host", e);
    }
    this.hostName = tmp;
  }

  @Override
  public void setWorkingDirectory(Path path) {

  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI name) throws IOException {
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    final File path = pathToFile(f);
    if (path.exists()) {
      return new LocalFileStatus(pathToFile(f), this);
    } else {
      throw new FileNotFoundException("File " + f + " does not exist or the user running "
          + "Flink ('" + System.getProperty("user.name")
          + "') has insufficient permissions to access it.");
    }
  }

  @Override
  public FileStatus[] listFiles(Path f) throws IOException {
    final File localf = pathToFile(f);
    FileStatus[] results;

    if (!localf.exists()) {
      return null;
    }
    if (localf.isFile()) {
      return new FileStatus[]{new LocalFileStatus(localf, this)};
    }

    final String[] names = localf.list();
    if (names == null) {
      return null;
    }
    results = new FileStatus[names.length];
    for (int i = 0; i < names.length; i++) {
      results[i] = getFileStatus(new Path(f, names[i]));
    }

    return results;
  }

  @Override
  public FSDataInputStream open(final Path f) throws IOException {
    final File file = pathToFile(f);
    return new LocalDataInputStream(file);
  }

  private File pathToFile(Path path) {
    Path curPath = path;
    if (!path.isAbsolute()) {
      curPath = new Path(getWorkingDirectory(), path);
    }
    return new File(curPath.toUri().getPath());
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
                                               long start, long len) throws IOException {
    return new BlockLocation[]{
        new LocalBlockLocation(hostName, file.getLen())
    };
  }

  //Newly added methods for HDFS -> Twister2 Integration
  private File pathToFile(Path path, int bufferSize) {
    Path curPath = path;
    if (!path.isAbsolute()) {
      curPath = new Path(getWorkingDirectory(), path);
    }
    return new File(curPath.toUri().getPath(), String.valueOf(bufferSize));
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    Path directoryPath = path;
    final File file = pathToFile(directoryPath, bufferSize);
    return new LocalDataInputStream(file);
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return null;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return new FileStatus[0];
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirsInternal(pathToFile(f));
  }

  private boolean mkdirsInternal(File file) throws IOException {
    if (file.isDirectory()) {
      return true;
    } else if (file.exists() && !file.isDirectory()) {
      throw new FileAlreadyExistsException(file.getAbsolutePath());
    } else {
      File parent = file.getParentFile();
      return (parent == null || mkdirsInternal(parent)) && file.mkdir();
    }
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public long getDefaultBlockSize() {
    return 0;
  }

  @Override
  public boolean isDistributedFS() {
    return false;
  }
}

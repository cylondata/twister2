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
package edu.iu.dsc.tws.data.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigConstants;
import edu.iu.dsc.tws.data.fs.local.LocalFileSystem;

/**
 * This class is an abstract of the generic file system that will be used in the system
 * This can be extend to support distributed file system or a local file system. This defines
 * the basic set of operations
 * that need to be supported by the concrete implementation
 */
public abstract class FileSystem {

  private static final Logger LOG = Logger.getLogger(FileSystem.class.getName());

  /**
   * Object used to protect calls to specific methods.
   */
  private static final Object SYNCHRONIZATION_OBJECT = new Object();

  /**
   * Data structure holding supported FileSystem Information
   */
  private static final Map<String, String> SUPPORTEDFS = new HashMap<String, String>();

  /**
   * The default filesystem scheme to be used. This can be specified by the parameter
   * <code>fs.default-scheme</code> in <code>flink-conf.yaml</code>. By default this is
   * set to <code>file:///</code>
   */
  private static URI defaultScheme;

  static {
    SUPPORTEDFS.put("file", LocalFileSystem.class.getName());
    //Newly added

  }

  /**
   * Returns a unsafe filesystem for the given uri
   */
  //private static FileSystem getFileSystem(URI uri) throws IOException {
  public static FileSystem getFileSystem(URI uri) throws IOException {
    FileSystem fs = null;
    URI asked = uri;
    URI curUri = uri;

    if (curUri == null) {
      throw new IOException("The URI " + curUri.toString() + " is not a vaild URI");
    }
    //TODO: check if the sycn is actually needed or can be scoped down
    synchronized (SYNCHRONIZATION_OBJECT) {

      if (curUri.getScheme() == null) {
        try {
          if (defaultScheme == null) {
            defaultScheme = new URI(ConfigConstants.DEFAULT_FILESYSTEM_SCHEME);
          }

          curUri = new URI(defaultScheme.getScheme(), null, defaultScheme.getHost(),
              defaultScheme.getPort(), curUri.getPath(), null, null);

        } catch (URISyntaxException e) {
          try {
            if (defaultScheme.getScheme().equals("file")) {
              curUri = new URI("file", null,
                  new Path(new File(curUri.getPath()).getAbsolutePath()).toUri().getPath(), null);
            }
          } catch (URISyntaxException ex) {
            // we tried to repair it, but could not. report the scheme error
            throw new IOException("The URI '" + curUri.toString() + "' is not valid.");
          }
        }
      }

      if (curUri.getScheme() == null) {
        throw new IOException("The URI '" + curUri + "' is invalid.\n"
            + "The fs.default-scheme = " + defaultScheme + ", the requested URI = " + asked
            + ", and the final URI = " + curUri + ".");
      }
      if (curUri.getScheme().equals("file") && curUri.getAuthority() != null
          && !curUri.getAuthority().isEmpty()) {
        String supposedUri = "file:///" + curUri.getAuthority() + curUri.getPath();

        throw new IOException("Found local file path with authority '"
            + curUri.getAuthority() + "' in path '"
            + curUri.toString()
            + "'. Hint: Did you forget a slash? (correct path would be '" + supposedUri + "')");
      }

      //TODO : need to add cache that can save FileSystem Objects and return from cache if available
      if (!isSupportedScheme(curUri.getScheme())) {
        //TODO: handle when the system is not supported
      } else {
        String fsClass = SUPPORTEDFS.get(curUri.getScheme());
        fs = instantiateFileSystem(fsClass);
        fs.initialize(curUri);
      }

    }
    return fs;
  }

  /**
   * Check if file
   */
  public boolean isFile(Path path) {
    return true;
  }

  /**
   * Check if directory
   */
  public boolean isDirectory(Path path) {
    return true;
  }

  /**
   * check if isSymlink
   */
  public boolean isSymlink(Path path) {
    return true;
  }

  /**
   * Set the working Directory
   */
  public abstract void setWorkingDirectory(Path path);

  /**
   * Get the working Directory
   */
  public abstract Path getWorkingDirectory();

  /**
   * Returns a URI whose scheme and authority identify this file system.
   *
   * @return a URI whose scheme and authority identify this file system
   */
  public abstract URI getUri();

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param name a {@link URI} whose authority section names the host, port, etc.
   * for this file system
   */
  public abstract void initialize(URI name) throws IOException;

  /**
   * <p>
   * Sets the default filesystem scheme based on the user-specified configuration parameter
   * <code>fs.default-scheme</code>. By default this is set to <code>file:///</code>
   * and the local filesystem is used.
   * <p>
   * As an example, if set to <code>hdfs://localhost:9000/</code>, then an HDFS deployment
   * with the namenode being on the local node and listening to port 9000 is going to be used.
   * In this case, a file path specified as <code>/user/USERNAME/in.txt</code>
   * is going to be transformed into <code>hdfs://localhost:9000/user/USERNAME/in.txt</code>. By
   * default this is set to <code>file:///</code> which points to the local filesystem.
   *
   * @param config the configuration from where to fetch the parameter.
   */
  public static void setDefaultScheme(Config config) throws IOException {
    synchronized (SYNCHRONIZATION_OBJECT) {
      if (defaultScheme == null) {
        String stringifiedUri = config.getStringValue(ConfigConstants.FILESYSTEM_SCHEME,
            ConfigConstants.DEFAULT_FILESYSTEM_SCHEME);
        try {
          defaultScheme = new URI(stringifiedUri);
        } catch (URISyntaxException e) {
          throw new IOException("The URI used to set the default filesystem "
              + "scheme ('" + stringifiedUri + "') is not valid.");
        }
      }
    }
  }

  /**
   * Returns a FileSystem for the given uri
   * TODO: need to think about security (Flink adds a safety net here, that is skipped for now)
   */
  public static FileSystem get(URI uri) throws IOException {
    return getFileSystem(uri);
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   * IOException see specific implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   */
  public abstract FileStatus[] listFiles(Path f) throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f the file to open
   */
  public abstract FSDataInputStream open(Path f) throws IOException;

  //private static FileSystem instantiateFileSystem(String className) throws IOException {
  public static FileSystem instantiateFileSystem(String className) throws IOException {
    try {
      Class<? extends FileSystem> fsClass = getFileSystemByName(className);
      return fsClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not load file system class '" + className + '\'', e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException("Could not instantiate file system class: " + e.getMessage(), e);
    }
  }

  /**
   * Checks if the given FileSystem scheme is currently supported
   *
   * @return true if supported, false otherwise
   */
  private static boolean isSupportedScheme(String scheme) {
    return SUPPORTEDFS.containsKey(scheme);
  }

  /**
   * Check if the given path exsits
   */
  public boolean exists(Path path) throws IOException {
    return true;
  }

  private static Class<? extends FileSystem> getFileSystemByName(String className)
      throws ClassNotFoundException {
    return Class.forName(className, true,
        FileSystem.class.getClassLoader()).asSubclass(FileSystem.class);
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file. For a nonexistent
   * file or regions, null will be returned.
   * This call is most helpful with DFS, where it returns
   * hostnames of machines that contain the given file.
   * The FileSystem will simply return an elt containing 'localhost'.
   */
  public abstract BlockLocation[] getFileBlockLocations(FileStatus file,
                                                        long start, long len) throws IOException;

  //Newly added methods for HDFS integration

  public abstract FSDataInputStream open(final Path f, final int bufferSize) throws IOException;

  public abstract FSDataOutputStream create(final Path f) throws IOException;

  public abstract boolean delete(final Path f, final boolean recursive) throws IOException;

  public abstract FileStatus[] listStatus(final Path f) throws IOException;

  public abstract boolean mkdirs(final Path f) throws IOException;

  public abstract boolean rename(final Path src, final Path dst) throws IOException;

  public abstract long getDefaultBlockSize();

  public abstract boolean isDistributedFS();



}

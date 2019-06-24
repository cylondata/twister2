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
package edu.iu.dsc.tws.data.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.ConfigConstants;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.fs.local.LocalFileSystem;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;

public final class FileSystemUtils {

  private FileSystemUtils() {
  }

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
    SUPPORTEDFS.put("hdfs", HadoopFileSystem.class.getName());
  }

  /**
   * For hadoop file system
   */
  public static FileSystem getFileSystem(URI uri, Config config) throws IOException {
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
        if ("hdfs".equals(curUri.getScheme())) {
          try {
            fs = instantiateFileSystem(fsClass, config);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException("No such method to invoke", e);
          } catch (InvocationTargetException e) {
            throw new RuntimeException("Invocation exception occured", e);
          }
          fs.initialize(curUri);
        } else {
          fs = instantiateFileSystem(fsClass);
          fs.initialize(curUri);
        }
      }
    }
    return fs;
  }

  /**
   * Returns a unsafe filesystem for the given uri
   */
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

  public static FileSystem get(Path path) throws IOException {
    return getFileSystem(path.toUri());
  }

  public static FileSystem get(Path path, Config config) throws IOException {
    return getFileSystem(path.toUri(), config);
  }

  public static FileSystem get(URI uri, Config config) throws IOException {
    return getFileSystem(uri, config);
  }

  private static Class<? extends FileSystem> getFileSystemByName(String className)
      throws ClassNotFoundException {
    return Class.forName(className, true,
        FileSystem.class.getClassLoader()).asSubclass(FileSystem.class);
  }


  private static FileSystem instantiateFileSystem(String className) throws IOException {
    try {
      Class<? extends FileSystem> fsClass = getFileSystemByName(className);
      return fsClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not load file system class '" + className + '\'', e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException("Could not instantiate file system class: " + e.getMessage(), e);
    }
  }

  private static FileSystem instantiateFileSystem(String className, Config config)
      throws IOException, NoSuchMethodException, InvocationTargetException {

    Class<?> fileSystemClass;
    try {
      Configuration conf = new Configuration(false);
      conf.addResource(
          new org.apache.hadoop.fs.Path(HdfsDataContext.getHdfsConfigDirectory(config)));
      conf.set("fs.defaultFS", HdfsDataContext.getHdfsUrlDefault(config));
      fileSystemClass = ClassLoader.getSystemClassLoader().loadClass(className);
      Constructor<?> classConstructor = fileSystemClass.getConstructor(
          Configuration.class, org.apache.hadoop.fs.FileSystem.class);
      Object newInstance = classConstructor.newInstance(new Object[]{conf,
          org.apache.hadoop.fs.FileSystem.get(conf)});
      return (FileSystem) newInstance;
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

}

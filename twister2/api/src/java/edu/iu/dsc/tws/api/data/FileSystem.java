//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

package edu.iu.dsc.tws.api.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.logging.Logger;

/**
 * This class is an abstract of the generic file system that will be used in the system
 * This can be extend to support distributed file system or a local file system. This defines
 * the basic set of operations
 * that need to be supported by the concrete implementation
 */
public abstract class FileSystem implements Serializable {

  private static final Logger LOG = Logger.getLogger(FileSystem.class.getName());

  public enum WriteMode {
    /**
     * Creates the target file only if no file exists at that path already.
     * Does not overwrite existing files and directories.
     */
    NO_OVERWRITE,

    /**
     * Creates a new target file regardless of any existing files or directories.
     * Existing files and directories will be deleted (recursively) automatically before
     * creating the new file.
     */
    OVERWRITE
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

  /**
   * Check if the given path exsits
   */
  public boolean exists(Path path) throws IOException {
    return true;
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

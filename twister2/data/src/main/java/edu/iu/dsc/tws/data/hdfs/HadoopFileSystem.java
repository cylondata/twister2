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
package edu.iu.dsc.tws.data.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import static edu.iu.dsc.tws.data.utils.PreConditions.checkNotNull;

public class HadoopFileSystem extends FileSystem {

  private static final Logger LOG = Logger.getLogger(HadoopFileSystem.class.getName());

  private org.apache.hadoop.conf.Configuration conf;
  private org.apache.hadoop.fs.FileSystem fileSystem;

  public HadoopFileSystem(
      org.apache.hadoop.conf.Configuration hadoopConfig,
      org.apache.hadoop.fs.FileSystem hadoopFileSystem) {

    this.conf = checkNotNull(hadoopConfig, "hadoopConfig");
    this.fileSystem = checkNotNull(hadoopFileSystem, "hadoopFileSystem");
  }

  public HadoopFileSystem() {
    super();
  }

  private static Class<? extends FileSystem> getFileSystemByName(String className)
      throws ClassNotFoundException {
    return Class.forName(className, true,
        FileSystem.class.getClassLoader()).asSubclass(FileSystem.class);
  }

  private static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toUri());
  }

  public org.apache.hadoop.fs.FileSystem getHadoopFileSystem() {
    return this.fileSystem;
  }

  private Configuration getHadoopConfiguration() {
    return new Configuration();
  }

  /**
   * Get the working Directory
   */
  @Override
  public Path getWorkingDirectory() {
    return new Path(this.fileSystem.getWorkingDirectory().toUri());
  }

  /**
   * Set the working Directory
   */
  @Override
  public void setWorkingDirectory(Path path1) {
  }


  @Override
  public URI getUri() {
    return fileSystem.getUri();
  }

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param name a {@link URI} whose authority section names the host, port, etc.
   * for this file system
   */
  @Override
  public void initialize(URI name) {
  }

  /**
   * It returns the status of the file respective to the path given by the user.
   * @param f The path we want information from
   * @return
   * @throws IOException
   */
  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    org.apache.hadoop.fs.FileStatus status = this.fileSystem.getFileStatus(toHadoopPath(f));
    return new HadoopFileStatus(status);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus file,
                                               final long start, final long len)
                                               throws IOException {
    if (!(file instanceof HadoopFileStatus)) {
      throw new IOException("file is not an instance of DistributedFileStatus");
    }

    final HadoopFileStatus f = (HadoopFileStatus) file;
    final org.apache.hadoop.fs.BlockLocation[] blkLocations =
        fileSystem.getFileBlockLocations(f.getInternalFileStatus(), start, len);
    final HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];
    for (int i = 0; i < distBlkLocations.length; i++) {
      distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
    }
    return distBlkLocations;
  }

  @Override
  public HadoopDataInputStream open(final Path f, final int bufferSize) throws IOException {
    final org.apache.hadoop.fs.Path directoryPath = toHadoopPath(f);
    final org.apache.hadoop.fs.FSDataInputStream fsDataInputStream =
        this.fileSystem.open(directoryPath, bufferSize);
    return new HadoopDataInputStream(fsDataInputStream);
  }

  /**
   * This method open and return the input stream object respective to the path
   * @param f Open an data input stream at the indicated path
   * @return
   * @throws IOException
   */
  @Override
  public HadoopDataInputStream open(final Path f) throws IOException {
    final org.apache.hadoop.fs.Path directoryPath = toHadoopPath(f);
    final org.apache.hadoop.fs.FSDataInputStream fsDataInputStream = fileSystem.open(directoryPath);
    return new HadoopDataInputStream(fsDataInputStream);
  }

  @Override
  public HadoopDataOutputStream create(final Path f) throws IOException {
    final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream =
        this.fileSystem.create(toHadoopPath(f));
    return new HadoopDataOutputStream(fsDataOutputStream);
  }

  public HadoopDataOutputStream append(Path path) throws IOException {
    final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream =
        this.fileSystem.append(toHadoopPath(path));
    return new HadoopDataOutputStream(fsDataOutputStream);
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    return this.fileSystem.delete(toHadoopPath(f), recursive);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return this.fileSystem.exists(toHadoopPath(f));
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    final org.apache.hadoop.fs.FileStatus[] hadoopFiles =
        this.fileSystem.listStatus(toHadoopPath(f));
    final FileStatus[] files = new FileStatus[hadoopFiles.length];

    for (int i = 0; i < files.length; i++) {
      files[i] = new HadoopFileStatus(hadoopFiles[i]);
    }
    return files;
  }

  @Override
  public boolean mkdirs(final Path f) throws IOException {
    return this.fileSystem.mkdirs(toHadoopPath(f));
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    return this.fileSystem.rename(toHadoopPath(src), toHadoopPath(dst));
  }

  @SuppressWarnings("deprecation")
  @Override
  public long getDefaultBlockSize() {
    return this.fileSystem.getDefaultBlockSize();
  }

  @Override
  public boolean isDistributedFS() {
    return true;
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   */
  @Override
  public FileStatus[] listFiles(Path f) {
    return new FileStatus[0];
  }

  public Class<?> getHadoopWrapperClassNameForFileSystem(String scheme) {
    Configuration hadoopConf = getHadoopConfiguration();
    Class<? extends org.apache.hadoop.fs.FileSystem> clazz = null;
    try {
      clazz = org.apache.hadoop.fs.FileSystem.getFileSystemClass(scheme, hadoopConf);
    } catch (IOException e) {
      LOG.info("Could not load the Hadoop File system implementation for scheme " + scheme);
    }
    return clazz;
  }

  public void close() throws IOException {
    this.fileSystem.close();
  }
}


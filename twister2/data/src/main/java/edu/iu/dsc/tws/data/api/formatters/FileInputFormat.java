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
package edu.iu.dsc.tws.data.api.formatters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputFormat;
import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileInputSplit;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.LocatableInputSplitAssigner;

/**
 * Base class for File input formats for specific file types the methods
 * {@link #nextRecord(Object)} and {@link #reachedEnd()} methods need to be implemented.
 */
public abstract class FileInputFormat<OT> implements InputFormat<OT, FileInputSplit> {
  private static final Logger LOG = Logger.getLogger(FileInputFormat.class.getName());

  private static final long serialVersionUID = 1L;

  /**
   * The desired number of splits, as set by the configure() method.
   */
  protected int numSplits = -1;

  /**
   * The splitLength is set to -1L for reading the whole split.
   */
  protected static final long READ_WHOLE_SPLIT_FLAG = -1L;

  /**
   * The flag to specify whether recursive traversal of the input directory
   * structure is enabled.
   */
  protected boolean enumerateNestedFiles = false;

  /**
   * The path to the file that contains the input.
   */
  protected Path filePath;

  /**
   * The fraction that the last split may be larger than the others.
   */
  protected static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

  /**
   * The minimal split size, set by the configure() method.
   */
  protected long minSplitSize = 0;

  /**
   * Current split that is used
   */
  protected FileInputSplit currentSplit;

  /**
   * Start point of the current split
   */
  protected long splitStart;

  /**
   * Length of the current split
   */
  protected long splitLength;

  /**
   * The input data stream
   */
  protected FSDataInputStream stream;

  /**
   * Time to wait when opening a file.
   */
  protected long openTimeout;

  public FileInputFormat() {
  }

  protected FileInputFormat(Path filePath) {
    this.filePath = filePath;
  }

  public boolean isEnumerateNestedFiles() {
    return enumerateNestedFiles;
  }

  public void setEnumerateNestedFiles(boolean enumerateNestedFiles) {
    this.enumerateNestedFiles = enumerateNestedFiles;
  }

  public long getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(long minSplitSize) {
    if (minSplitSize < 0) {
      throw new IllegalArgumentException("The minimum split size cannot be negative.");
    }

    this.minSplitSize = minSplitSize;
  }

  public int getNumSplits() {
    return numSplits;
  }

  public void setNumSplits(int numSplits) {
    if (numSplits < -1 || numSplits == 0) {
      throw new IllegalArgumentException("The desired number of splits must "
          + "be positive or -1 (= don't care).");
    }

    this.numSplits = numSplits;
  }

  public Path getFilePath() {
    return filePath;
  }

  public void setFilePath(Path filePath) {
    this.filePath = filePath;
  }

  @Override

  public void configure(Config parameters) {

  }

  /**
   * Computes the input splits for the file. By default, one file block is one split. If more splits
   * are requested than blocks are available, then a split may be a fraction of a
   * block and splits may cross block boundaries.
   *
   * @param minNumSplits The minimum desired number of file splits.
   * @return The computed file splits.
   */
  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws Exception {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }

    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(curminNumSplits);

    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<FileStatus>();
    long totalLength = 0;

    final FileSystem fs = path.getFileSystem();
    final FileStatus pathFile = fs.getFileStatus(path);

    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      //TODO L3: implement test for unsplittable
      //testForUnsplittable(pathFile);

      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    //TODO L3: Handle if unsplittable
    //TODO L1: check if we can add the i j method when making splits so that the last split is not
    // larger than the other splits
    final long maxSplitSize = totalLength / curminNumSplits
        + (totalLength % curminNumSplits == 0 ? 0 : 1);

    //Generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();

      final long localminSplitSize;
      if (this.minSplitSize <= blockSize) {
        localminSplitSize = this.minSplitSize;
      } else {
        LOG.log(Level.WARNING, "Minimal split size of " + this.minSplitSize
            + " is larger than the block size of " + blockSize
            + ". Decreasing minimal split size to block size.");
        localminSplitSize = blockSize;
      }

      final long splitSize = Math.max(localminSplitSize, Math.min(maxSplitSize, blockSize));
      final long halfSplit = splitSize >>> 1;

      final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
      if (len > 0) {
        // get the block locations and make sure they are in order with respect to their offset
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);

        long bytesUnassigned = len;
        long position = 0;

        int blockIndex = 0;
        while (bytesUnassigned > maxBytesForLastSplit) {
          // get the block containing the majority of the data
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          // create a new split
          FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position, splitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += splitSize;
          bytesUnassigned -= splitSize;
        }

        if (bytesUnassigned > 0) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), position,
              bytesUnassigned, blocks[blockIndex].getHosts());
          inputSplits.add(fis);
        }

      } else {
        // special case with a file of zero bytes size
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
        String[] hosts;
        if (blocks.length > 0) {
          hosts = blocks[0].getHosts();
        } else {
          hosts = new String[0];
        }
        final FileInputSplit fis = new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
        inputSplits.add(fis);
      }
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }


  @Override
  public LocatableInputSplitAssigner getInputSplitAssigner(FileInputSplit[] inputSplits) {
    return new LocatableInputSplitAssigner(inputSplits);
  }

  /**
   * Opens an input stream to the file defined in the input format.
   * The stream is positioned at the beginning of the given split.
   * <p>
   * The stream is actually opened in an asynchronous thread to make sure any
   * interruptions to the thread
   * working on the input format do not reach the file system.
   */
  @Override
  public void open(FileInputSplit fileSplit) throws IOException {

    this.currentSplit = fileSplit;
    this.splitStart = fileSplit.getStart();
    this.splitLength = fileSplit.getLength();

    LOG.log(Level.INFO, "Opening input split " + fileSplit.getPath() + " ["
        + this.splitStart + "," + this.splitLength + "]");

    // open the split in an asynchronous thread
    final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit, this.openTimeout);
    isot.start();

    try {
      this.stream = isot.waitForCompletion();
      //TODO L3: Check the need to input stream wrapper ( ex for decoding ).
      // This is not an initial requirement
      //this.stream = decorateInputStream(this.stream, fileSplit);
    } catch (Throwable t) {
      throw new IOException("Error opening the Input Split " + fileSplit.getPath()
          + " [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
    }

    // get FSDataInputStream
    if (this.splitStart != 0) {
      this.stream.seek(this.splitStart);
    }
  }


  @Override
  public boolean reachedEnd() throws IOException {
    return false;
  }

  @Override
  public OT nextRecord(OT reuse) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
   *
   * @return the total length of accepted files.
   */
  protected long sumFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
      throws IOException {
    final FileSystem fs = path.getFileSystem();

    long length = 0;

    for (FileStatus file : fs.listFiles(path)) {
      if (file.isDir()) {
        if (acceptFile(file) && enumerateNestedFiles) {
          length += sumFilesInDir(file.getPath(), files, logExcludedFiles);
        } else {
          if (logExcludedFiles) {
            LOG.log(Level.INFO, "Directory " + file.getPath().toString() + " did not pass the "
                + "file-filter and is excluded.");
          }
        }
      } else {
        if (acceptFile(file)) {
          files.add(file);
          length += file.getLen();
          //TODO: implement test for unsplittable
          //testForUnsplittable(file);
        } else {
          if (logExcludedFiles) {
            LOG.log(Level.INFO, "Directory " + file.getPath().toString()
                + " did not pass the file-filter and is excluded.");
          }
        }
      }
    }
    return length;
  }

  /**
   * A simple hook to filter files and directories from the input.
   * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
   * same filters by default.
   *
   * @param fileStatus The file status to check.
   * @return true, if the given file or directory is accepted
   */
  public boolean acceptFile(FileStatus fileStatus) {
    final String name = fileStatus.getPath().getName();
    return !name.startsWith("_")
        && !name.startsWith(".");
    //TODO: Need to add support for file filters
    // && !filesFilter.filterPath(fileStatus.getTarget());
  }

  /**
   * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of
   * the file described by the given
   * offset.
   *
   * @param blocks The different blocks of the file. Must be ordered by their offset.
   * @param offset The offset of the position in the file.
   * @param startIndex The earliest index to look at.
   * @return The index of the block containing the given position (gives the block that contains
   * most of the split)
   */
  protected int getBlockIndexForPosition(BlockLocation[] blocks, long offset,
                                         long halfSplitSize, int startIndex) {
    // go over all indexes after the startIndex
    for (int i = startIndex; i < blocks.length; i++) {
      long blockStart = blocks[i].getOffset();
      long blockEnd = blockStart + blocks[i].getLength();

      if (offset >= blockStart && offset < blockEnd) {
        // got the block where the split starts
        // check if the next block contains more than this one does
        if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
          return i + 1;
        } else {
          return i;
        }
      }
    }
    throw new IllegalArgumentException("The given offset is not contained in the any block.");
  }

  /**
   * Obtains a DataInputStream in an thread that is not interrupted.
   * This is a necessary hack around the problem that the HDFS client is very
   * sensitive to InterruptedExceptions.
   */
  public static class InputSplitOpenThread extends Thread {

    private final FileInputSplit split;

    private final long timeout;

    private volatile FSDataInputStream fdis;

    private volatile Throwable error;

    private volatile boolean aborted;

    public InputSplitOpenThread(FileInputSplit split, long timeout) {
      super("Transient InputSplit Opener");
      setDaemon(true);

      this.split = split;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        final FileSystem fs = FileSystem.get(this.split.getPath().toUri());
        this.fdis = fs.open(this.split.getPath());

        // check for canceling and close the stream in that case, because no one will obtain it
        if (this.aborted) {
          final FSDataInputStream f = this.fdis;
          this.fdis = null;
          f.close();
        }
      } catch (Throwable t) {
        this.error = t;
      }
    }

    public FSDataInputStream waitForCompletion() throws Throwable {
      final long start = System.currentTimeMillis();
      long remaining = this.timeout;

      do {
        try {
          // wait for the task completion
          this.join(remaining);
        } catch (InterruptedException iex) {
          // we were canceled, so abort the procedure
          abortWait();
          throw iex;
        }
      }
      while (this.error == null && this.fdis == null
          && (remaining = this.timeout + start - System.currentTimeMillis()) > 0);

      if (this.error != null) {
        throw this.error;
      }
      if (this.fdis != null) {
        return this.fdis;
      } else {
        // double-check that the stream has not been set by now. we don't know here whether
        // a) the opener thread recognized the canceling and closed the stream
        // b) the flag was set such that the stream did not see it and we have a valid stream
        // In any case, close the stream and throw an exception.
        abortWait();

        final boolean stillAlive = this.isAlive();
        final StringBuilder bld = new StringBuilder(256);
        for (StackTraceElement e : this.getStackTrace()) {
          bld.append("\tat ").append(e.toString()).append('\n');
        }
        throw new IOException("Input opening request timed out. Opener was "
            + (stillAlive ? "" : "NOT ")
            + " alive. Stack of split open thread:\n" + bld.toString());
      }
    }

    /**
     * Double checked procedure setting the abort flag and closing the stream.
     */
    private void abortWait() {
      this.aborted = true;
      final FSDataInputStream inStream = this.fdis;
      this.fdis = null;
      if (inStream != null) {
        try {
          inStream.close();
        } catch (Throwable t) {
        }
      }
    }
  }
}

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
package edu.iu.dsc.tws.data.api.splits;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

/**
 * A file input split provides information on a particular part of a file, possibly
 * hosted on a distributed file system and replicated among several hosts.
 */
public abstract class FileInputSplit<OT> extends LocatableInputSplit<OT> {

  private static final Logger LOG = Logger.getLogger(FileInputSplit.class.getName());

  private static final long serialVersionUID = 1L;

  /**
   * The path of the file this file split refers to.
   */
  private final Path file;

  /**
   * The position of the first byte in the file to process.
   */
  private long start;

  /**
   * The number of bytes in the file to process.
   */
  private long length;

  /**
   * The desired number of splits, as set by the configure() method.
   */
  protected int numSplits = -1;

  /**
   * The flag to specify whether recursive traversal of the input directory
   * structure is enabled.
   */
  protected boolean enumerateNestedFiles = false;

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

  /**
   * Constructs a split with host information.
   *
   * @param num the number of this input split
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
   * @param hosts the list of hosts containing the block, possibly <code>null</code>
   */
  public FileInputSplit(int num, Path file, long start, long length, String[] hosts) {
    super(num, hosts);

    this.file = file;
    this.start = start;
    this.length = length;
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

  // --------------------------------------------------------------------------------------------

  /**
   * Returns the path of the file containing this split's data.
   *
   * @return the path of the file containing this split's data.
   */
  public Path getPath() {
    return file;
  }

  /**
   * Returns the position of the first byte in the file to process.
   *
   * @return the position of the first byte in the file to process
   */
  public long getStart() {
    return start;
  }

  /**
   * Returns the number of bytes in the file to process.
   *
   * @return the number of bytes in the file to process
   */
  public long getLength() {
    return length;
  }

  // --------------------------------------------------------------------------------------------

  @Override
  public int hashCode() {
    return getSplitNumber() ^ (file == null ? 0 : file.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj != null && obj instanceof FileInputSplit && super.equals(obj)) {
      FileInputSplit other = (FileInputSplit) obj;

      return this.start == other.start
          && this.length == other.length
          && (this.file == null ? other.file == null : (other.file != null
              && this.file.equals(other.file)));
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "[" + getSplitNumber() + "] " + file + ":" + start + "+" + length;
  }

  public void configure(Config parameters) {
  }

  @Override
  public void close() throws IOException {

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
  public void open() throws IOException {
    this.splitStart = getStart();
    this.splitLength = getLength();

    LOG.log(Level.INFO, "Opening input split " + getPath() + " ["
        + splitStart + "," + splitLength + "]");

    // open the split in an asynchronous thread
    final InputSplitOpenThread isot = new InputSplitOpenThread(this, this.openTimeout);
    isot.start();

    try {
      this.stream = isot.waitForCompletion();
      //TODO L3: Check the need to input stream wrapper ( ex for decoding ).
      // This is not an initial requirement
      //this.stream = decorateInputStream(this.stream, fileSplit);
    } catch (Throwable t) {
      throw new IOException("Error opening the Input Split " + getPath()
          + " [" + splitStart + "," + splitLength + "]: " + t.getMessage(), t);
    }

    // get FSDataInputStream
    if (this.splitStart != 0) {
      this.stream.seek(this.splitStart);
    }
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
        } catch (Throwable ignore) {
        }
      }
    }
  }
}

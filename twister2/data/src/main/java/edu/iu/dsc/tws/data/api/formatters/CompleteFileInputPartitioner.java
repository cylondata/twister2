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
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;

/**
 * Base class for File input formats for specific file types the methods
 */
public abstract class CompleteFileInputPartitioner<OT>
    implements InputPartitioner<OT, FileInputSplit<OT>> {

  private static final Logger LOG = Logger.getLogger(FileInputPartitioner.class.getName());

  private static final long serialVersionUID = 1L;

  /**
   * The desired number of splits, as set by the configure() method.
   */
  protected int numSplits = -1;

  /**
   * The splitLength is set to -1L for reading the whole split.
   */
  public static final long READ_WHOLE_SPLIT_FLAG = -1L;

  /**
   * The flag to specify whether recursive traversal of the input directory
   * structure is enabled.
   */
  private boolean enumerateNestedFiles = false;

  /**
   * The path to the file that contains the input.
   */
  protected Path filePath;

  protected Config config;

  /**
   * The fraction that the last split may be larger than the others.
   */
  private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

  /**
   * The minimal split size, set by the configure() method.
   */
  private long minSplitSize = 0;

  /**
   * The input data stream
   */
  protected FSDataInputStream stream;


  public CompleteFileInputPartitioner(Path filePath) {
    this.filePath = filePath;
  }

  public CompleteFileInputPartitioner(Path filePath, Config cfg) {
    this.filePath = filePath;
    this.config = cfg;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }

  /**
   * It creates the split for the complete file.
   *
   * @param minNumSplits Number of minimal input splits, as a hint.
   */
  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws Exception {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }

    int curminNumSplits = Math.max(minNumSplits, this.numSplits);
    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);
    List<FileStatus> files = new ArrayList<>();

    long totalLength = 0;

    final FileSystem fs = path.getFileSystem(config);
    final FileStatus pathFile = fs.getFileStatus(path);

    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    final long maxSplitSize = totalLength;

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
      if (len > 0) {
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);
        long position = 0;
        int blockIndex = 0;
        for (int i = 0; i < curminNumSplits; i++) {
          blockIndex = getBlockIndexForPosition(blocks, position, splitSize, blockIndex);
          FileInputSplit fis = createSplit(splitNum++, file.getPath(), position, splitSize,
              blocks[blockIndex].getHosts());
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
        for (int i = 0; i < curminNumSplits; i++) {
          final FileInputSplit fis = createSplit(splitNum++, file.getPath(), 0, 0, hosts);
          inputSplits.add(fis);
        }
      }
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  protected abstract FileInputSplit createSplit(int num, Path file, long start,
                                                long length, String[] hosts);


  /**
   * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
   *
   * @return the total length of accepted files.
   */
  long sumFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
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
   * The method may be overridden. Hadoop's FileInputPartitioner has a similar mechanism and applies the
   * same filters by default.
   *
   * @param fileStatus The file status to check.
   * @return true, if the given file or directory is accepted
   */
  private boolean acceptFile(FileStatus fileStatus) {
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
  int getBlockIndexForPosition(BlockLocation[] blocks, long offset,
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
}

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
public abstract class FileInputPartitioner<OT> implements InputPartitioner<OT, FileInputSplit<OT>> {
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


  public FileInputPartitioner(Path filePath) {
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
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws Exception {
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
          FileInputSplit fis = createSplit(splitNum++, file.getPath(), position, splitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += splitSize;
          bytesUnassigned -= splitSize;
        }

        if (bytesUnassigned > 0) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = createSplit(splitNum++, file.getPath(), position,
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
        final FileInputSplit fis = createSplit(splitNum++, file.getPath(), 0, 0, hosts);
        inputSplits.add(fis);
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

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
import java.util.logging.Logger;

import edu.iu.dsc.tws.data.api.assigner.LocatableInputSplitAssigner;
import edu.iu.dsc.tws.data.api.splits.BinaryInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.BlockLocation;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

/**
 * Input formatter class that reads binary files
 */
public class BinaryInputPartitioner extends FileInputPartitioner<byte[]> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(BinaryInputPartitioner.class.getName());

  /**
   * The length of a single record in the given binary file.
   */
  protected transient int recordLength;

  public BinaryInputPartitioner(Path filePath, int recordLen) {
    super(filePath);
    this.recordLength = recordLen;
  }

  /**
   * Computes the input splits for the file. By default, one file block is one split. If more
   * splits are requested than blocks are available, then a split may be a fraction of a block and
   * splits may cross block boundaries.
   *
   * @param minNumSplits The minimum desired number of file splits.
   * @return The computed file splits.
   */
  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }
    //TODO L2: The current implementaion only handles a snigle binary file not a set of files
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
    //Splits will be made so that records are not broken into two splits
    //Odd records will be divided among the first splits so the max diff would be 1 record
    if (totalLength % this.recordLength != 0) {
      throw new IllegalStateException("The Binary file has a incomplete record");
    }
    long numberOfRecords = totalLength / this.recordLength;
    long minRecordsForSplit = Math.floorDiv(numberOfRecords, minNumSplits);
    long oddRecords = numberOfRecords % minNumSplits;

    //Generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();
      final long minSplitSize = minRecordsForSplit * this.recordLength;
      long currentSplitSize = minSplitSize;
      long halfSplit = currentSplitSize >>> 1;

      if (oddRecords > 0) {
        currentSplitSize = currentSplitSize + this.recordLength;
        oddRecords--;
      }

      if (len > 0) {
        // get the block locations and make sure they are in order with respect to their offset
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);

        long bytesUnassigned = len;
        long position = 0;

        int blockIndex = 0;
        while (bytesUnassigned >= currentSplitSize) {
          // get the block containing the majority of the data
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          // create a new split
          FileInputSplit fis = new BinaryInputSplit(splitNum++, file.getPath(),
              position, currentSplitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += currentSplitSize;
          bytesUnassigned -= currentSplitSize;
        }
      } else {
        throw new IllegalStateException("The binary file " + file.getPath() + " is Empty");
      }

    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(FileInputSplit<byte[]>[] inputSplits) {
    return new LocatableInputSplitAssigner(inputSplits);
  }

  @Override
  protected FileInputSplit createSplit(int num, Path file, long start,
                                       long length, String[] hosts) {
    return null;
  }
}

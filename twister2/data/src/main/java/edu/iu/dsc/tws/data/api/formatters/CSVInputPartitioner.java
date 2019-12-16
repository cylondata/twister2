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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.BlockLocation;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.assigner.LocatableInputSplitAssigner;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class CSVInputPartitioner extends FileInputPartitioner<Object> {

  private static final long serialVersionUID = -1L;

  private static final Logger LOG = Logger.getLogger(CSVInputPartitioner.class.getName());

  protected transient int recordLength;
  protected transient int numSplits;

  public  CSVInputPartitioner(Path filePath, int recordLen) {
    super(filePath);
    this.recordLength = recordLen;
  }

  public CSVInputPartitioner(Path filePath, int recordLen, Config cfg) {
    super(filePath);
    this.recordLength = recordLen;
    this.configure(cfg);
  }

  public CSVInputPartitioner(Path filePath, int recordLen, int numberOfTasks) {
    super(filePath);
    this.numSplits = numberOfTasks;
    this.recordLength = recordLen;
  }

  public CSVInputPartitioner(Path filePath, int recordLen, int numberOfTasks, Config config) {
    super(filePath, config);
    this.numSplits = numberOfTasks;
    this.recordLength = recordLen;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }

  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }
    int currentMinimumNumSplits = Math.max(minNumSplits, this.numSplits);
    System.out.println("The file path is:" + this.filePath);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(currentMinimumNumSplits);
    List<FileStatus> files = new ArrayList<FileStatus>();
    long totalLength = 0;

    final FileSystem fs = FileSystemUtils.get(path);
    final FileStatus pathFile = fs.getFileStatus(path);

    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    LOG.info("total length of the file:" + totalLength + "\t" + this.recordLength);

    if (totalLength % this.recordLength != 0) {
      throw new IllegalStateException("The file has a incomplete record");
    }

    long numberOfRecords = totalLength / this.recordLength;
    long minRecordsForSplit = Math.floorDiv(numberOfRecords, minNumSplits);
    long oddRecords = numberOfRecords % minNumSplits;

    //Generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {
      final long len = file.getLen();
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
          FileInputSplit fis = new CSVInputSplit(splitNum++, file.getPath(), position,
              currentSplitSize, blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += currentSplitSize;
          bytesUnassigned -= currentSplitSize;
        }

        if (bytesUnassigned > 0) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = new CSVInputSplit(splitNum++, filePath, position,
              bytesUnassigned, blocks[blockIndex].getHosts());
          inputSplits.add(fis);
        }
      } else {
        throw new IllegalStateException("The csv file " + file.getPath() + " is Empty");
      }
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  @Override
  public InputSplitAssigner<Object> getInputSplitAssigner(FileInputSplit<Object>[] inputSplits) {
    return new LocatableInputSplitAssigner<>(inputSplits);
  }

  @Override
  protected FileInputSplit createSplit(int num, Path file, long start,
                                       long length, String[] hosts) {
    return null;
  }
}

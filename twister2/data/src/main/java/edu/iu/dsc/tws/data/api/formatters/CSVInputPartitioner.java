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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.BlockLocation;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.assigner.LocatableInputSplitAssigner;
//import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public class CSVInputPartitioner extends FileInputPartitioner<byte[]> {

  private static final Logger LOG = Logger.getLogger(CSVInputPartitioner.class.getName());

  private static final long serialVersionUID = -1L;

  private boolean enumerateNestedFiles = false;

  protected transient int numSplits;

  protected Path filePath;
  protected Config config;

  private long minSplitSize = 0;

  private transient int recordLength;

  public CSVInputPartitioner(Path filepath) {
    super(filepath);
    this.filePath = filepath;
  }

  public CSVInputPartitioner(Path filepath, int numberOfTasks) {
    super(filepath);
    this.numSplits = numberOfTasks;
  }

  public CSVInputPartitioner(Path filepath, int numberOfTasks, Config cfg) {
    super(filepath, cfg);
    this.numSplits = numberOfTasks;
  }

  public CSVInputPartitioner(Path filepath, Config cfg) {
    super(filepath, cfg);
    this.filePath = filepath;
    this.config = cfg;
  }

  public CSVInputPartitioner(Path filepath, int recordLen, int numberOfTasks, Config cfg) {
    super(filepath, cfg);
    this.filePath = filepath;
    this.config = cfg;
    this.numSplits = numberOfTasks;
    this.recordLength = recordLen;
  }


  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }

  /**
   * It create the number of splits based on the task parallelism value.
   *
   * @param minNumSplits Number of minimal input splits, as a hint.
   * @return
   * @throws IOException
   */
  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }

    LOG.info("number of splits:" + minNumSplits);
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);
    long totalLength = 0;

    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);
    final Path path = this.filePath;
    final FileSystem fs = FileSystemUtils.get(path, config);
    final FileStatus pathFile = fs.getFileStatus(path);

    List<FileStatus> files = new ArrayList<>();
    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

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

      final long maxSplitSize = totalLength;
      final long splitSize = Math.max(localminSplitSize, Math.min(maxSplitSize, blockSize));
      if (len > 0) {
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks);
        long position = 0;
        int blockIndex = 0;
        for (int i = 0; i < curminNumSplits; i++) {
          blockIndex = getBlockIndexForPosition(blocks, position, splitSize, blockIndex);
          FileInputSplit fis = new CSVInputSplit(splitNum++, file.getPath(), position, splitSize,
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
          final FileInputSplit fis = new CSVInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
          inputSplits.add(fis);
        }
      }
    }

    /*int splitNum = 0;
    final long maxSplitSize = totalLength;
    for (final FileStatus file : files) {
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();
      final long localminSplitSize;

      final int numberOfLines = (int) file.getLen();
      LOG.info("number of lines:" + numberOfLines);
      if (this.minSplitSize <= blockSize) {
        localminSplitSize = this.minSplitSize;
      } else {
        LOG.log(Level.WARNING, "Minimal split size of "
            + this.minSplitSize + " is larger than the block size of "
            + blockSize + ". Decreasing minimal split size to block size.");
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
          FileInputSplit fis = new CSVInputSplit(splitNum++, file.getPath(), position, splitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);
        }
      } else {
        LOG.info("I am coming inside the else block");
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
        String[] hosts;
        if (blocks.length > 0) {
          hosts = blocks[0].getHosts();
        } else {
          hosts = new String[0];
        }
        for (int i = 0; i < curminNumSplits; i++) {
          FileInputSplit fis = new CSVInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
          inputSplits.add(fis);
        }
      }
    }*/
    LOG.info("input splits value:" + inputSplits.size() + "\t"
        + Arrays.toString(inputSplits.toArray()));
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  @Override
  public InputSplitAssigner<byte[]> getInputSplitAssigner(FileInputSplit<byte[]>[] inputSplits) {
    return new LocatableInputSplitAssigner(inputSplits);
  }

  @Override
  protected FileInputSplit createSplit(int num, Path file, long start, long length,
                                       String[] hosts) {
    return null;
  }
}

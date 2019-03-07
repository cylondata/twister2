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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.FSDataInputStream;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public abstract class FixedInputPartitioner<OT>
    implements InputPartitioner<OT, FileInputSplit<OT>> {

  private static final Logger LOG = Logger.getLogger(FixedInputPartitioner.class.getName());

  protected int numSplits = -1;
  public static final long READ_WHOLE_SPLIT_FLAG = -1L;
  private boolean enumerateNestedFiles = false;
  protected Path filePath;
  protected Config config;
  private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;
  private long minSplitSize = 0;
  protected FSDataInputStream stream;

  public FixedInputPartitioner(Path filePath) {
    this.filePath = filePath;
  }

  public FixedInputPartitioner(Path filePath, Config cfg) {
    this.filePath = filePath;
    this.config = cfg;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }


  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws Exception {

    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(curminNumSplits);

    // get all the files that are involved in the splits
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

    //Generate the splits
    final long maxSplitSize = totalLength / curminNumSplits
        + (totalLength % curminNumSplits == 0 ? 0 : 1);

    for (final FileStatus file : files) {

      //First Split Calculation
      final long lineCount = Files.lines(Paths.get(file.getPath().getPath())).count();
      int splSize = (int) (lineCount / curminNumSplits);
      long totalbytes = getSplitSize(file.getPath().getPath(), 0, splSize);

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
      final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
      long bytesUnassigned = len;

      int splitNum = 0;
      int position = 0;
      int splitSize = 5;
      int unassignedLines = 10;
      while (unassignedLines >= totalLines) {
        String[] hosts = new String[0];
        final FileInputSplit fis
            = createSplit(splitNum++, file.getPath(), position, splitSize, hosts);
        inputSplits.add(fis);
        position += splitSize;
        unassignedLines -= splitSize;
      }
      if (unassignedLines > 0) {
        LOG.info("un assigned lines are::::" + unassignedLines);
        String[] hosts = new String[0];
        final FileInputSplit fis
            = createSplit(splitNum++, file.getPath(), position, totalLines, hosts);
        inputSplits.add(fis);
      }
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

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

  protected abstract FileInputSplit createSplit(int num, Path file, long start,
                                                long length, String[] hosts);

  private boolean acceptFile(FileStatus fileStatus) {
    final String name = fileStatus.getPath().getName();
    return !name.startsWith("_")
        && !name.startsWith(".");
    //TODO: Need to add support for file filters
    // && !filesFilter.filterPath(fileStatus.getTarget());
  }

  @Override
  public InputSplitAssigner<OT> getInputSplitAssigner(FileInputSplit<OT>[] inputSplits) {
    return null;
  }
}

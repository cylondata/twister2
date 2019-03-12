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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

/**
 * The fixed input partitioner is really useful if the user exactly knows the number of lines
 * to be generated or available in the input data file.
 */
public abstract class FixedInputPartitioner<OT>
    implements InputPartitioner<OT, FileInputSplit<OT>> {

  private static final Logger LOG = Logger.getLogger(FixedInputPartitioner.class.getName());

  private int numSplits = -1;
  private boolean enumerateNestedFiles = false;
  protected Path filePath;
  protected Config config;
  private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;
  private long minSplitSize = 0;
  protected FSDataInputStream stream;
  private int dataSize = 0;

  public FixedInputPartitioner(Path filePath) {
    this.filePath = filePath;
  }

  public FixedInputPartitioner(Path filePath, Config cfg) {
    this.filePath = filePath;
    this.config = cfg;
  }

  public FixedInputPartitioner(Path filePath, Config cfg, int datasize) {
    this.filePath = filePath;
    this.config = cfg;
    this.dataSize = datasize;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }


  /**
   * This method create the input splits which is based on the number of lines in the input and the
   * parallelism value.
   *
   * @param minNumSplits Number of minimal input splits, as a hint.
   */
  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws Exception {

    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);

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
      //To retrieve the total count of the number of the lines in a file.
      //final long lineCount = Files.lines(Paths.get(file.getPath().getPath())).count();
      final long lineCount = dataSize;
      int splSize = (int) (lineCount / curminNumSplits);

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

      long totalbytes = getSplitSize(fs, file.getPath(), 0, splSize);
      final long splitSize = Math.max(localminSplitSize, Math.min(maxSplitSize, blockSize));
      final long maxBytesForLastSplit = (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);
      long bytesUnassigned = len;
      int splitNum = 0;
      int position = 0;

      if (len > 0) {
        while (bytesUnassigned > maxBytesForLastSplit) {
          String[] hosts = new String[0];
          final FileInputSplit fis
              = createSplit(splitNum++, file.getPath(), position, totalbytes, hosts);
          inputSplits.add(fis);
          position += totalbytes;
          bytesUnassigned -= totalbytes;
        }
        if (bytesUnassigned > 0) {
          long remainingBytes = getSplitSize(fs, file.getPath(), splSize, dataSize);
          String[] hosts = new String[0];
          final FileInputSplit fis
              = createSplit(splitNum++, file.getPath(), position, bytesUnassigned, hosts);
          inputSplits.add(fis);
        }
      } else {
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

  /**
   * This method calculate the split size corresponding to the start and end position.
   */
  private long getSplitSize(FileSystem fileSystem, Path filename, int start, long end)
      throws IOException {
    int count = start;
    long totalBytes = 0L;
    String line;
    BufferedReader in = new BufferedReader(new InputStreamReader(
        fileSystem.open(filename), StandardCharsets.UTF_8));
    byte[] b;
    int currentLine = 1;
    while ((line = in.readLine()) != null) {
      if (currentLine <= start) {
        ++currentLine;
        continue;
      }
      b = line.getBytes(StandardCharsets.UTF_8);
      totalBytes += b.length;
      count++;
      if (count == end) {
        break;
      }
    }
    return totalBytes;
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

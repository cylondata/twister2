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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.BlockLocation;
import edu.iu.dsc.tws.api.data.FSDataInputStream;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

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
    LOG.info("I am coming inside to configure the parameters");
    this.config = parameters;
  }


  /**
   * This method create the input splits which is based on the number of lines in the input and the
   * parallelism value.
   *
   * @param minNumSplits Number of minimal input splits, as a hint.
   */
  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws IOException {

    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);

    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<>();

    long totalLength = 0;
    final FileSystem fs = FileSystemUtils.get(path);
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

    if (files.size() > 1) {
      throw new IllegalStateException("FixedInputPartitioner does not support multiple files"
          + "currently");
    }
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
      int currLineCount = 0;
      long[] splitSizes = getSplitSizes(fs, file.getPath(), curminNumSplits, splSize);

      int position = 0;
      if (len > 0) {
        for (int i = 0; i < splitSizes.length; i++) {
          String[] hosts = new String[0];
          final FileInputSplit fis
              = createSplit(i, file.getPath(), position, splitSizes[i], hosts);
          position += splitSizes[i];
          inputSplits.add(fis);
        }
      } else {
        //TODO need to check this section of the code for correctness
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
        String[] hosts;
        if (blocks.length > 0) {
          hosts = blocks[0].getHosts();
        } else {
          hosts = new String[0];
        }
        final FileInputSplit fis = createSplit(0, file.getPath(), 0, 0, hosts);
        inputSplits.add(fis);
      }


      //Old code that does splitting based on fixed byte sizes
      /*      final long splitSize = Math.max(localminSplitSize, Math.min(maxSplitSize, blockSize));
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
      }*/
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  /**
   * Calculates the sizes of the splits for the given file, each split is created to be the
   * exact amount of files that are specified in splitSlize
   *
   * @param fs File system that is used to read the file
   * @param filename the path to the file
   * @param numberOfSplits the number of splits to make
   * @param splitSize the number of lines in each split
   * @return a long array of split sizes
   */
  private long[] getSplitSizes(FileSystem fs, Path filename, int numberOfSplits, int splitSize)
      throws IOException {
    long[] splits = new long[numberOfSplits];
    long currentSplitBytes = 0L;
    int currLineCount = 0;
    int completeSplitCount = 0;
    String line;
    BufferedReader in = new BufferedReader(new InputStreamReader(
        fs.open(filename), StandardCharsets.UTF_8));
    byte[] b;

    for (int i = 0; i < numberOfSplits; i++) {
      currLineCount = 0;
      currentSplitBytes = 0;
      while (currLineCount < splitSize && (line = in.readLine()) != null) {
        b = line.getBytes(StandardCharsets.UTF_8);
        //Adding 1 byte for the EOL
        currentSplitBytes += b.length + 1;
        currLineCount++;
      }
      splits[i] = currentSplitBytes;
      if (currLineCount == splitSize) {
        completeSplitCount++;
      }
    }
    if (completeSplitCount != numberOfSplits) {
      throw new IllegalStateException(String.format("The file %s could not be split into"
          + " %d splits with %d lines for each split,"
          + " please check the input file sizes", filename.toString(), numberOfSplits, splitSize));
    }
    return splits;
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
      //Adding 1 byte for the EOL
      totalBytes += b.length + 1;
      count++;
      if (count == end) {
        break;
      }
    }
    return totalBytes;
  }

  long sumFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
      throws IOException {
    final FileSystem fs = FileSystemUtils.get(path);
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

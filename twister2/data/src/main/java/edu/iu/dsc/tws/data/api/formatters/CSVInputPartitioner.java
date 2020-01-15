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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.BlockLocation;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.splits.CSVInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;

public abstract class CSVInputPartitioner<OT> implements InputPartitioner<OT, FileInputSplit<OT>> {

  private static final Logger LOG = Logger.getLogger(CSVInputPartitioner.class.getName());

  private static final long serialVersionUID = -1L;

  protected transient int numSplits;

  protected Path filePath;
  protected Config config;

  private int dataSize = 0;

  private long minSplitSize = 0;

  private boolean enumerateNestedFiles = false;

  public CSVInputPartitioner(Path filePath, Config cfg) {
    this.filePath = filePath;
    this.config = cfg;
  }

  public CSVInputPartitioner(Path filePath, Config cfg, int datasize) {
    this.filePath = filePath;
    this.config = cfg;
    this.dataSize = datasize;
  }

  @Override
  public void configure(Config parameters) {
    this.config = parameters;
  }

  @Override
  public FileInputSplit<OT>[] createInputSplits(int minNumSplits) throws IOException {

    // take the desired number of splits into account
    int curminNumSplits = Math.max(minNumSplits, this.numSplits);

    final Path path = this.filePath;
    final List<FileInputSplit> inputSplits = new ArrayList<>(curminNumSplits);

    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<>();
    final FileSystem fs = FileSystemUtils.get(path);
    final FileStatus pathFile = fs.getFileStatus(path);

    long totalLength = 0;
    if (pathFile.isDir()) {
      totalLength += sumFilesInDir(path, files, true);
    } else {
      files.add(pathFile);
      totalLength += pathFile.getLen();
    }

    if (files.size() > 1) {
      throw new IllegalStateException("FixedInputPartitioner does not support multiple files"
          + "currently");
    }
    for (final FileStatus file : files) {
      final long lineCount = dataSize;
      int splSize = (int) (lineCount / curminNumSplits);
      final long len = file.getLen();
      long[] splitSizes = getSplitSizes(fs, file.getPath(), curminNumSplits, splSize);
      int position = 0;
      if (len > 0) {
        for (int i = 0; i < splitSizes.length; i++) {
          String[] hosts = new String[0];
          final FileInputSplit fis
              = new CSVInputSplit(i, file.getPath(), position, splitSizes[i], hosts);
          position += splitSizes[i];
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
        final FileInputSplit fis = new CSVInputSplit(0, file.getPath(), 0, 0, hosts);
        inputSplits.add(fis);
      }
    }
    return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
  }

  private long[] getSplitSizes(FileSystem fs, Path filename, int numberOfSplits, int splitSize)
      throws IOException {
    long[] splits = new long[numberOfSplits];
    long currentSplitBytes = 0L;
    int currLineCount = 0;
    int completeSplitCount = 0;
    BufferedInputStream bis = new BufferedInputStream(fs.open(filename));
    boolean skipLf = true;
    int overflow = -1;
    outerfor:
    for (int i = 0; i < numberOfSplits; i++) {
      currLineCount = 0;
      currentSplitBytes = 0;
      int c;
      char ch;
      linewhile:
      while (currLineCount < splitSize) {
        if (overflow != -1) {
          c = overflow;
          ch = (char) c;
          overflow = -1;
        } else {
          c = bis.read();
        }
        if (c == -1) {
          //reached end of stream
          break outerfor;
        } else {
          currentSplitBytes++;
          ch = (char) c;
          if (skipLf) {
            skipLf = false;
            if (ch == '\n') {
              continue linewhile;
            }
          }

          if (ch == '\r' || ch == '\n') {
            currLineCount++;
            if (ch == '\r') {
              if (currLineCount == splitSize) {
                c = bis.read();
                if (c == -1) {
                  //reached end of stream
                  break outerfor;
                } else {
                  ch = (char) c;
                  if (ch == '\n') {
                    currentSplitBytes++;
                    continue linewhile;
                  } else {
                    overflow = c;
                  }
                }
              } else {
                skipLf = true;
              }
            }
          }
        }
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

  protected abstract FileInputSplit createSplit(int num, Path file, long start,
                                                long length, String[] hosts);

  @Override
  public InputSplitAssigner<OT> getInputSplitAssigner(FileInputSplit<OT>[] inputSplits) {
    return null;
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

  private boolean acceptFile(FileStatus fileStatus) {
    final String name = fileStatus.getPath().getName();
    return !name.startsWith("_")
        && !name.startsWith(".");
  }
}

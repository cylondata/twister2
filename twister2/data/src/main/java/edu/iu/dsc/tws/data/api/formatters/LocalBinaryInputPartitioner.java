package edu.iu.dsc.tws.data.api.formatters;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.assigner.LocatableInputSplitAssigner;
import edu.iu.dsc.tws.data.api.splits.BinaryInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class LocalBinaryInputPartitioner extends BinaryInputPartitioner {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(LocalTextInputPartitioner.class.getName());

  private int numberOfTasks;

  private Config config;

  private int recordLength;

  private LocatableInputSplitAssigner assigner;

  public LocalBinaryInputPartitioner(Path filePath, int recordLen) {
    super(filePath, recordLen);
    this.recordLength = recordLen;
  }

  public LocalBinaryInputPartitioner(Path filePath, int nTasks, int recordLen, Config cfg) {
    super(filePath, recordLen, nTasks);
    this.numberOfTasks = nTasks;
    this.recordLength = recordLen;
    this.config = cfg;
  }

  protected BinaryInputSplit createSplit(int num, Path file, long start,
                                         long length, String[] hosts) {
    return new BinaryInputSplit(num, file, start, length, hosts);
  }

  public InputSplitAssigner getInputSplitAssigner(FileInputSplit[] inputSplits) {
    if (assigner == null) {
      assigner = new LocatableInputSplitAssigner(inputSplits);
    }
    return assigner;
  }
}

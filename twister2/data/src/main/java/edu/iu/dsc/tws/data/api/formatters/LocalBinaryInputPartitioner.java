package edu.iu.dsc.tws.data.api.formatters;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.assigner.OrderedInputSplitAssigner;
import edu.iu.dsc.tws.data.api.splits.BinaryInputSplit;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;

public class LocalBinaryInputPartitioner extends BinaryInputPartitioner {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(LocalTextInputPartitioner.class.getName());

  private int nTasks;

  private Config config;

  private OrderedInputSplitAssigner assigner;

  public LocalBinaryInputPartitioner(Path filePath, int numTasks, int records) {
    //super(filePath, numTasks);
    super(filePath, records);
    this.nTasks = numTasks;
  }

  public LocalBinaryInputPartitioner(Path filePath, int numTasks, Config cfg) {
    super(filePath, numTasks);
    this.nTasks = numTasks;
    this.config = cfg;
  }

  @Override
  protected BinaryInputSplit createSplit(int num, Path file, long start,
                                         long length, String[] hosts) {
    return new BinaryInputSplit(num, file, start, length, hosts);
  }

  public OrderedInputSplitAssigner getInputSplitAssigner(
      FileInputSplit[] inputSplits) {
    if (assigner == null) {
      assigner = new OrderedInputSplitAssigner(inputSplits, nTasks);
    }
    return assigner;
  }
}


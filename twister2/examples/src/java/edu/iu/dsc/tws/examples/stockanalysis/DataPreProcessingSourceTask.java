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
package edu.iu.dsc.tws.examples.stockanalysis;

import java.util.Date;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Context;

public class DataPreProcessingSourceTask extends BaseSource {
  private static final Logger LOG = Logger.getLogger(DataPreProcessingSourceTask.class.getName());

  private String dataInputFile;
  private String vectorDirectory;
  private int numberOfDays;
  private Date startDate;
  private Date endDate;
  private int mode;

  public DataPreProcessingSourceTask(String datainputfile, String vectordirectory,
                                     String numberofdays, String startdate,
                                     String enddate, String mode) {
    this.dataInputFile = datainputfile;
    this.vectorDirectory = vectordirectory;
    this.numberOfDays = Integer.parseInt(numberofdays);
    this.mode = Integer.parseInt(mode);
  }

  @Override
  public void execute() {
    context.write(Context.TWISTER2_DIRECT_EDGE, "DataProcessing");
    try {
      Thread.sleep(50000);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted Exception Occured");
    }
  }
}

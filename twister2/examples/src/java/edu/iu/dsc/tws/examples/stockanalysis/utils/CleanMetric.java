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
package edu.iu.dsc.tws.examples.stockanalysis.utils;

/**
 * It is responsible for keeping track of bad data points
 */
public class CleanMetric {
  public int negativeCount;
  public int missingValues;
  public int properSplitData;
  public int nonProperSplitData;
  public int constantStock;
  public int totalStocks;
  public int invalidStocks;
  public int stocksWithIncorrectDays;
  public int lenghtWrong;
  public int dupRecords;
  public int writtenStocks;

  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append("Negative: ").append(negativeCount).append(" ");
    sb.append("MissingValues: ").append(missingValues).append(" ");
    sb.append("ProperSplitData: ").append(properSplitData).append(" ");
    sb.append("NonProperSplitData: ").append(nonProperSplitData).append(" ");
    sb.append("ConstantStock: ").append(constantStock).append(" ");
    sb.append("InvalidStock: ").append(invalidStocks).append(" ");
    sb.append("TotalStock: ").append(totalStocks).append(" ");
    sb.append("IncorrectDaysStocks: ").append(stocksWithIncorrectDays).append(" ");
    sb.append("Lenght: ").append(lenghtWrong).append(" ");
    sb.append("Duprecords exceeded: ").append(dupRecords).append(" ");
    sb.append("Written stocks: ").append(writtenStocks).append(" ");
    return sb.toString();
  }
}

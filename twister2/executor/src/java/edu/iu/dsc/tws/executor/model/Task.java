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
package edu.iu.dsc.tws.executor.model;


import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public class Task implements Serializable {



  public String getThreadId() {

    return threadId;
  }

  public void setThreadId(String threadId) {
    this.threadId = threadId;
  }

  public Task(long id, String name, Date date, String description, String threadId) {

    this.id = id;
    this.name = name;
    this.date = date;
    this.description = description;
    this.threadId = threadId;
  }

  private long id;
  private String name;
  private Date date;
  private String description;
  private String threadId;

  public Task(long id, String name, Date date, String description) {
    this.id = id;
    this.name = name;
    this.date = date;
    this.description = description;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Date getDate() {
    return date;
  }

  public String getDescription() {
    return description;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public void setDescription(String description) {
    this.description = description;
  }


}

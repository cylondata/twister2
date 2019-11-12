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

function doPost(e) {
    var charts = [];
    var csvFiles = [];

    var text = "";

    Logger.log(e.postData.contents);
    var results = JSON.parse(e.postData.contents);
    Object.keys(results).forEach(function (id) {
        var benchmark = results[id];
        if (!benchmark.failed) {
            text += (benchmark.title + "\t: Passed\n");
            charts.push(createChart(benchmark.title, benchmark.values));

            // csv file
            if (benchmark.csv) {
                var folder = DriveApp.getFolderById("1VcUySRNj4pVrhvIDSAYXwfFBNdDioLMt");
                var file = folder.createFile(id + ".csv", benchmark.csv);
                csvFiles.push(file.getAs(MimeType.CSV));
                DriveApp.removeFile(file);
            }
        } else {
            text += (benchmark.title + "\t: Failed\n");
        }
    });

    MailApp.sendEmail("twister2@googlegroups.com", "Benchmark Results - [" + new Date().toLocaleDateString("en-US") + "]",
        text + "\nBenchmark charts(if available) are attached with this email.\n\nRegards,\nTwister2 Team.\n\n\nNOTE : This is a system generated email.", {
            attachments: charts.concat(csvFiles),
            name: 'Twitser2 Nightly Benchmark'
        });

    return ContentService.createTextOutput('I just successfully handled your POST request.');
}

function createChart(title, dataPoints) {
    var dataBuilder = Charts.newDataTable()
        .addColumn(Charts.ColumnType.NUMBER, 'Data Size(kb)')
        .addColumn(Charts.ColumnType.NUMBER, 'Average Latency(ns)');

    var sorted = Object.keys(dataPoints).sort(function (a, b) {
        return parseInt(a) - parseInt(b)
    });

    sorted.forEach(function (dataSize) {
        dataBuilder.addRow([dataSize / 256, dataPoints[dataSize] / 1000 / 1000]);
    });

    var chart = Charts.newLineChart().setDataTable(dataBuilder).setTitle(title)
        .setXAxisTitle("Data Size (kb)")
        .setYAxisTitle("Average Latency (µs)")
        .setLegendPosition(Charts.Position.BOTTOM)
        .setCurveStyle(Charts.CurveStyle.SMOOTH)
        .setPointStyle(Charts.PointStyle.MEDIUM)
        .setDimensions(1024, 768)
        .build();

    return chart.getAs(MimeType.JPEG);
}

function callChart() {

    doPost({
        "parameter": {},
        "contextPath": "",
        "contentLength": 73,
        "queryString": "",
        "parameters": {},
        "postData": {
            "type": "application/json",
            "length": 73,
            "contents": '{"comms_reduce":{"title":"Comms Reduce","failed":true},"comms_bcast":{"title":"Comms Broadcast","values":{"256":"1.34769502E9"}}}',
            "name": "postData"
        }
    });
}



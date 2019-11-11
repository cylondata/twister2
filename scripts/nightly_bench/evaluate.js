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

const csv = require("csv-parse");
const fs = require('fs');
const axios = require('axios');

var resultsReady = 0;

const results = {};

const twister2JobsDir = "";
const googleScriptUrl = "";

const benchmarks = {
    "comms_reduce": {
        title: "Comms Reduce",
        path: twister2JobsDir + "/edu.iu.dsc.tws.examples.comms.stream.SReduceExample/comms_reduce.csv"
    },
    "comms_bcast": {
        title: "Comms Broadcast",
        path: twister2JobsDir + "/edu.iu.dsc.tws.examples.comms.stream.SBroadcastExample/comms_bcast.csv"
    },
    "comms_all_gather": {
        title: "Comms AllGather",
        path: twister2JobsDir + "/edu.iu.dsc.tws.examples.comms.stream.SAllGatherExample/comms_all_gather.csv"
    },
    "comms_all_reduce": {
        title: "Comms AllReduce",
        path: twister2JobsDir + "/edu.iu.dsc.tws.examples.comms.stream.SAllReduceExample/comms_all_reduce.csv"
    },
    "comms_gather": {
        title: "Comms Gather",
        path: +twister2JobsDir + "/edu.iu.dsc.tws.examples.comms.stream.SGatherExample/comms_gather.csv"
    }
};

function isReady() {
    if (++resultsReady == Object.keys(benchmarks).length) {
        //post
        axios.post(googleScriptUrl,
            results).then(() => {
            console.log("Posted")
        }).catch(err => {
            console.error("Error in posting", err)
        });
    }
}

function process(bmId, bmObj) {
    fs.access(bmObj.path, (err) => {
        if (err) {
            results[bmId] = {
                title: bmObj.title,
                failed: true
            };
            isReady();
        } else {
            results[bmId] = {
                csv: fs.readFileSync(bmObj.path).toString(),
                title: bmObj.title,
                values: {}
            };

            let dataSizeIndex = -1;
            let timeIndex = -1;
            fs.createReadStream(bmObj.path)
                .pipe(csv({haders: true}))
                .on('headers', (headers) => {
                    console.log("headers", headers);
                    headers.forEach((header, index) => {
                        console.log(header);
                        if (header === "Data Size") {
                            dataSizeIndex = index;
                        } else if (header === "Total Time (ns)") {
                            timeIndex = index;
                        }
                    })
                })
                .on('data', (data) => {
                    if (data.indexOf("Data Size") > 0) {
                        data.forEach((header, index) => {
                            if (header === "Data Size") {
                                dataSizeIndex = index;
                            } else if (header === "Total Time (ns)") {
                                timeIndex = index;
                            }
                        })
                    } else {
                        results[bmId].values[data[dataSizeIndex]] = data[timeIndex];
                    }
                })
                .on('end', () => {
                    isReady();
                });
        }
    });
}

Object.keys(benchmarks).forEach(key => {
    process(key, benchmarks[key]);
});

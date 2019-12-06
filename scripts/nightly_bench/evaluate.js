const csv = require("csv-parse");
const fs = require('fs');
const axios = require('axios');

var resultsReady = 0;

const results = {};

const googleScriptURL = "";

const benchmarks = {
    "s_comms_reduce": {
        title: "[STREAM] Comms Reduce",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.stream.SReduceExample/comms_reduce.csv",
        averageColumn: true
    },
    "s_comms_bcast": {
        title: "[STREAM] Comms Broadcast",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.stream.SBroadcastExample/comms_bcast.csv",
        averageColumn: true
    },
    "s_comms_all_gather": {
        title: "[STREAM] Comms AllGather",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.stream.SAllGatherExample/comms_all_gather.csv",
        averageColumn: true
    },
    "s_comms_all_reduce": {
        title: "[STREAM] Comms AllReduce",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.stream.SAllReduceExample/comms_all_reduce.csv",
        averageColumn: true
    },
    "s_comms_gather": {
        title: "[STREAM] Comms Gather",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.stream.SGatherExample/comms_gather.csv",
        averageColumn: true
    },
    "b_comms_reduce": {
        title: "[BATCH] Comms Reduce",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BReduceExample/comms_reduce.csv",
        averageColumn: false
    },
    "b_comms_bcast": {
        title: "[BATCH] Comms Broadcast",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BBroadcastExample/comms_bcast.csv",
        averageColumn: false
    },
    "b_comms_all_gather": {
        title: "[BATCH] Comms AllGather",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BAllGatherExample/comms_all_gather.csv",
        averageColumn: false
    },
    "b_comms_all_reduce": {
        title: "[BATCH] Comms AllReduce",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BAllReduceExample/comms_all_reduce.csv",
        averageColumn: false
    },
    "b_comms_gather": {
        title: "[BATCH] Comms Gather",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BGatherExample/comms_gather.csv",
        averageColumn: false
    },
    "b_comms_keyed_gather": {
        title: "[BATCH] Comms Keyed Gather",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BKeyedGatherExample/comms_keyed_gather.csv",
        averageColumn: false
    },
    "b_comms_keyed_reduce": {
        title: "[BATCH] Comms Keyed Reduce",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BKeyedReduceExample/comms_keyed_reduce.csv",
        averageColumn: false
    },
    "b_comms_keyed_partition": {
        title: "[BATCH] Comms Keyed Partition",
        path: "/N/u/cwidanage/.twister2/jobs/edu.iu.dsc.tws.examples.comms.batch.BKeyedPartitionExample/comms_keyed_partition.csv",
        averageColumn: false
    }
};

function isReady() {
    if (++resultsReady == Object.keys(benchmarks).length) {
        //post
        axios.post(googleScriptURL,
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
                failed: true,
                averageColumn: bmObj.averageColumn
            };
            isReady();
        } else {
            results[bmId] = {
                csv: fs.readFileSync(bmObj.path).toString(),
                title: bmObj.title,
                values: {},
                averageColumn: bmObj.averageColumn
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
                        } else if (!bmObj.averageColumn && header === "Total Time (ns)") {
                            timeIndex = index;
                        } else if (bmObj.averageColumn && header === "Average Time (ns)") {
                            timeIndex = index;
                        }
                    })
                })
                .on('data', (data) => {
                    if (data.indexOf("Data Size") > 0) {
                        data.forEach((header, index) => {
                            if (header === "Data Size") {
                                dataSizeIndex = index;
                            } else if (!bmObj.averageColumn && header === "Total Time (ns)") {
                                timeIndex = index;
                            } else if (bmObj.averageColumn && header === "Average Time (ns)") {
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

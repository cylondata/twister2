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
import React from "react";
import "./WorkerInfoComponent.css";
import {Button, ButtonGroup, Card, Tag} from "@blueprintjs/core";
import saveAs from 'file-saver';
import {WorkerService} from "../../../services/WorkerService";
import {ComputeResourceCard} from "../compute-resource/ComputeResourceCard";
import NodeTag from "../nodes/NodeTag";
import ClusterTag from "../clusters/ClusterTag";
import {Link} from "react-router-dom";

export default class WorkerInfoComponent extends React.Component {

    constructor(props) {
        super(props);

        this.logs = `
[2018-10-22 09:50:22] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils: Loading configuration with twister2_home: /twister2 and configuration: /twister2-memory-dir/twister2-job/kubernetes  
[2018-10-22 09:50:22] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils: Loaded: 98 parameters from configuration directory: /twister2-memory-dir/twister2-job/kubernetes  
[2018-10-22 09:50:22] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: Getting the pod list for the namespace: default  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: Number of pods in the received list: 8  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: nfs-client-provisioner-66bbfb9f76-d82jv  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: t2-job-0  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: t2-job-1 
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: t2-job-job-master-0  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: Received pod Running event for the pod: t2-job-job-master-0[10.34.0.0]  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerStarter: Job master address: 10.34.0.0  
[2018-10-22 09:50:23] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerStarter: NodeInfo for this worker: NodeInfo{nodeIP='149.165.150.83', rackName='null', dataCenterName='null'}
Starting to wait for the job package to arrive ...
Job package arrived fully to the pod.
Starting edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerStarter .... 
[2018-10-22 14:18:33] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils: Loading configuration with twister2_home: /twister2 and configuration: /twister2-memory-dir/twister2-job/kubernetes  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils: Loaded: 98 parameters from configuration directory: /twister2-memory-dir/twister2-job/kubernetes  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: Getting the pod list for the namespace: default  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: Number of pods in the received list: 7  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: hello-node-6b5f5f97f8-sbjmk  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: hello-world-job-0  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: hello-world-job-1
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: hello-world-job-job-master-0  
[2018-10-22 14:18:34] [INFO] edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils: my-nginx-6fbb694897-d5zcr  
        `.split("\n").filter(log => log.trim() !== "");

        this.state = {
            logLines: this.logs,
            codeData: "",
            autoUpdate: true,
            autoScroll: true,
            worker: undefined
        };

        console.log(this.props);

        this.timer = -1;

        this.codeRef = null;

    }

    loadWorker = () => {
        WorkerService.getWorkerById(this.props.match.params.jobId, this.props.match.params.workerId).then(response => {
            this.setState({
                worker: response.data
            })
        })
    };

    startLogAppending = () => {
        this.timer = setInterval(() => {
            let currentLogs = this.state.logLines;
            let toAppend = Math.floor(Math.random() * 5);

            for (let i = 0; i < toAppend; i++) {
                if (currentLogs.length > 250) {
                    currentLogs.shift();
                }
                currentLogs.push(this.logs[Math.floor(Math.random() * this.logs.length)]);
            }

            this.setState({
                logLines: currentLogs,
                codeData: window.PR.prettyPrintOne(currentLogs.join("\n"), "bash")
            }, () => {
                if (this.state.autoScroll) {
                    let codeDiv = document.getElementById("code-div");
                    if (codeDiv) {
                        codeDiv.scrollTop = codeDiv.scrollHeight;
                    }
                }
            });
        }, 500);
    };


    stopLogAppending = () => {
        clearInterval(this.timer);
    };

    componentWillUnmount() {
        this.stopLogAppending();
    }

    componentDidMount() {
        this.startLogAppending();
        this.loadWorker();
    }

    toggleAutoUpdate = () => {
        let autoUpdate = !this.state.autoUpdate;
        this.setState({
            autoUpdate: autoUpdate
        });
        if (!autoUpdate) {
            this.stopLogAppending();
        } else {
            this.startLogAppending()
        }
    };


    toggleAutoScroll = () => {
        let autoScroll = !this.state.autoScroll;
        this.setState({
            autoScroll: autoScroll
        });
    };


    downloadLogs = () => {
        let blob = new Blob([this.state.logLines.join("\n")], {type: "text/plain;charset=utf-8"});
        saveAs(blob, "worker1-logs.txt");
    };

    render() {
        if (!this.state.worker) {
            return null;
        }

        return (
            <div>
                <h1 className="t2-page-heading">
                    <Link to={`/jobs/${this.state.worker.job.jobID}`}>
                        {this.state.worker.job.jobName}</Link>/{this.state.worker.workerID}
                </h1>
                <table className="bp3-html-table bp3-html-table-striped" width="100%">
                    <tbody>
                    <tr>
                        <td>
                            Worker ID
                        </td>
                        <td>
                            {this.state.worker.workerID}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Worker IP/Port
                        </td>
                        <td>
                            {this.state.worker.workerIP}:{this.state.worker.workerPort}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Cluster
                        </td>
                        <td>
                            <ClusterTag cluster={this.state.worker.node.cluster}/>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Node
                        </td>
                        <td>
                            <NodeTag node={this.state.worker.node}/>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            State
                        </td>
                        <td>
                            <Tag minimal={true}
                                 intent={this.state.workerStateIntent}>{this.state.worker.state}</Tag>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Heartbeat
                        </td>
                        <td>
                            {this.state.worker.heartbeatTime}
                        </td>
                    </tr>
                    <tr>
                        <td>Compute Resource</td>
                        <td>
                            <ComputeResourceCard cr={this.state.worker.computeResource}/>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Ports
                        </td>
                        <td>
                            {this.state.worker.workerPorts.map(wp => {
                                return (
                                    <Tag large={true}
                                         minimal={true}
                                         className="tw-worker-ports-tag">{wp.label} : {wp.port}</Tag>
                                );
                            })}
                        </td>
                    </tr>
                    </tbody>
                </table>
                <Card>
                    <h4>Logs</h4>
                    <div>
                        <ButtonGroup>
                            <Button text="Auto Update" icon="automatic-updates"
                                    active={this.state.autoUpdate}
                                    onClick={this.toggleAutoUpdate}/>
                            <Button text="Auto Scroll" icon="sort-asc"
                                    active={this.state.autoScroll}
                                    onClick={this.toggleAutoScroll}/>
                            <Button text="Download" icon="cloud-download"
                                    onClick={this.downloadLogs}/>
                        </ButtonGroup>
                    </div>
                    <div>
                        <pre className="prettyprint" id="code-div">
                            <code className="bash">
                                <div dangerouslySetInnerHTML={{__html: this.state.codeData}}/>
                            </code>
                        </pre>
                    </div>
                </Card>
            </div>
        )
    }
}
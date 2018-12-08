import React from "react";
import {Card, Elevation, Icon, Button, Tag} from "@blueprintjs/core";
import "./WorkerCard.css";
import {Link} from "react-router-dom";
import JobTag from "../../workloads/jobs/JobTag";
import ClusterCard from "../clusters/ClusterCard";
import ClusterTag from "../clusters/ClusterTag";
import NodeTag from "../nodes/NodeTag";
import {ComputeResourceCard} from "../compute-resource/ComputeResourceCard";

export default class WorkerCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            worker: this.props.worker
        }
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="ninja" iconSize={40} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        {this.state.worker.job.jobName.toUpperCase()}/{this.state.worker.workerID}
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Job
                            </td>
                            <td>
                                <JobTag job={this.state.worker.job}/>
                            </td>
                        </tr>
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
                                <Tag minimal={true}>{this.state.worker.state}</Tag>
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
                                <table className="bp3-html-table">
                                    <thead>
                                    <tr>
                                        <td>Label</td>
                                        <td>Port</td>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    {this.state.worker.workerPorts.map(wp => {
                                        return (
                                            <tr>
                                                <td>{wp.label}</td>
                                                <td>{wp.port}</td>
                                            </tr>
                                        );
                                    })}
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    <div className="tw-node-actions">
                        <Link to="/workers/worker1">
                            <Button icon="info-sign">Logs</Button>
                        </Link>
                        <Button icon="refresh">Sync</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
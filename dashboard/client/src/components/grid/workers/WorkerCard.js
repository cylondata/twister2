import React from "react";
import {Button, Card, Elevation, Icon, Tag, Intent} from "@blueprintjs/core";
import "./WorkerCard.css";
import {Link} from "react-router-dom";
import JobTag from "../../workloads/jobs/JobTag";
import ClusterTag from "../clusters/ClusterTag";
import NodeTag from "../nodes/NodeTag";
import {ComputeResourceCard} from "../compute-resource/ComputeResourceCard";
import {WorkerService} from "../../../services/WorkerService";
import {DashToaster} from "../../Dashboard";

export default class WorkerCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            worker: this.props.worker,
            workerSyncing: false,
            workerStateIntent: this.getStateIntent(this.props.worker.state)
        }
    }

    getStateIntent = (state) => {
        switch (state) {
            case "COMPLETED":
                return Intent.SUCCESS;
            case "KILLED":
                return Intent.DANGER;
            case "NOT_PINGING":
                return Intent.WARNING;
            case "FAILED":
                return Intent.DANGER;
            default:
                return Intent.NONE;
        }
    };

    setWorkerSyncing = (syncing) => {
        this.setState({
            workerSyncing: syncing
        })
    };

    syncWorker = () => {
        this.setWorkerSyncing(true);
        WorkerService.getWorkerById(
            this.state.worker.job.jobID,
            this.state.worker.workerID
        ).then(response => {
            this.setState({
                worker: response.data,
                workerSyncing: false,
                workerStateIntent: this.getStateIntent(response.data.state)
            });
            DashToaster.show({
                message: "Successfully synced worker " + this.state.worker.workerID +
                    " of Job " + this.state.worker.job.jobID,
                intent: Intent.SUCCESS
            });
        }).catch(err => {
            this.setWorkerSyncing(false);
            DashToaster.show({
                message: "Failed to sync worker " + this.state.worker.workerID +
                    " of Job " + this.state.worker.job.jobID,
                intent: Intent.DANGER
            });
        });
    };

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
                        <Button icon="refresh" onClick={this.syncWorker}>Sync</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
import React from "react";
import {Button, Card, Collapse, Elevation, Icon, Intent, Position, Tag, Tooltip} from "@blueprintjs/core";
import "./WorkerCard.css";
import {Link} from "react-router-dom";
import ClusterTag from "../clusters/ClusterTag";
import NodeTag from "../nodes/NodeTag";
import {ComputeResourceCard} from "../compute-resource/ComputeResourceCard";
import {WorkerService} from "../../../services/WorkerService";
import {DashToaster} from "../../Dashboard";
import WorkerStateTag from "./WorkerStateTag";

export default class WorkerCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            worker: this.props.worker,
            workerSyncing: false,
            workerStateIntent: this.getStateIntent(this.props.worker.state),
            infoOpen: false
        }
    }

    getStateIntent = (state) => {
        switch (state) {
            case "COMPLETED":
                return Intent.SUCCESS;
            case "KILLED":
                return Intent.WARNING;
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

    toggleInfo = () => {
        this.setState({
            infoOpen: !this.state.infoOpen
        })
    };

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-table-row tw-worker-row">
                <div className="tw-worker-row-summary">
                    <div className="tw-worker-row-icon">
                        <Icon icon="ninja" iconSize={18}/>
                    </div>

                    <Link to={`/jobs/${this.state.worker.job.jobID}/${this.state.worker.workerID}`}>
                        <div className="tw-table-row-left">
                            <div className="tw-job-row-name">
                                {this.state.worker.job.jobName}/{this.state.worker.workerID}
                            </div>
                        </div>
                    </Link>
                    <div className="tw-table-row-right">
                        <Tooltip content="Worker ID" position={Position.TOP}>
                            <Tag intent={Intent.NONE} icon="tag" minimal={true} style={{height: 25}}
                                 className="tw-worker-row-status-tag">{this.state.worker.workerID}</Tag>
                        </Tooltip>
                        <Tooltip content="Heartbeat" position={Position.TOP}>
                            <Tag icon="pulse" style={{height: 25}} intent={Intent.NONE} minimal={true}>
                                {new Date(this.state.worker.heartbeatTime).toLocaleString()}
                            </Tag>
                        </Tooltip>
                        <WorkerStateTag state={this.state.worker.state}/>
                        <Tooltip content="Sync" position={Position.TOP}>
                            <Button icon="refresh" onClick={this.syncWorker} small={true} minimal={true}/>
                        </Tooltip>
                        <Button rightIcon="caret-down" text="" small={true} minimal={true}
                                onClick={this.toggleInfo}/>
                    </div>
                </div>
                <div className="tw-worker-row-info">
                    <Collapse isOpen={this.state.infoOpen} keepChildrenMounted={true}>
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
                                    {
                                        this.state.worker.state !== "KILLED_BY_SCALE_DOWN" &&
                                        <Tag minimal={true}
                                             intent={this.state.workerStateIntent}>{this.state.worker.state}</Tag>
                                    }
                                    {
                                        this.state.worker.state === "KILLED_BY_SCALE_DOWN" &&
                                        <Tag minimal={true}
                                             className="tw-worker-state-tag-killed-bsd"
                                             intent={this.state.workerStateIntent}>{this.state.worker.state}</Tag>
                                    }
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
                                    {this.state.worker.workerPorts.map((wp, index) => {
                                        return (
                                            <Tag large={true}
                                                 minimal={true}
                                                 key={index}
                                                 className="tw-worker-ports-tag">{wp.label} : {wp.port}</Tag>
                                        );
                                    })}
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </Collapse>
                </div>
            </Card>
        )
    }
}
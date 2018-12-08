import React from "react";
import {Card, Elevation, Icon, Button, Tag} from "@blueprintjs/core";
import "./JobCard.css";
import {ComputeResourceCard} from "../../grid/compute-resource/ComputeResourceCard";
import NodeTag from "../../grid/nodes/NodeTag";
import WorkerTag from "../../grid/workers/WorkerTag";
import ClusterTag from "../../grid/clusters/ClusterTag";

export default class JobCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            job: this.props.job
        };
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="new-grid-item" iconSize={40} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        {this.state.job.jobName.toUpperCase()}
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Worker Class
                            </td>
                            <td>
                                {this.state.job.workerClass}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Cluster
                            </td>
                            <td>
                                <ClusterTag cluster={this.state.job.node.cluster}/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Node
                            </td>
                            <td>
                                <NodeTag node={this.state.job.node}/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Number of Workers
                            </td>
                            <td>
                                {this.state.job.numberOfWorkers}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Workers
                            </td>
                            <td>
                                {
                                    this.state.job.workers.map(worker => {
                                        return <WorkerTag worker={worker}/>
                                    })
                                }
                            </td>
                        </tr>
                        <tr>
                            <td>
                                State
                            </td>
                            <td>
                                <Tag minimal={true}>{this.state.job.state}</Tag>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Compute Resources
                            </td>
                            <td>
                                {this.state.job.computeResources.map(cr => {
                                    return <ComputeResourceCard cr={cr} index={cr.index}/>
                                })}
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    <div className="tw-node-actions">
                        {this.props.finished &&
                        <Button icon="play">Start</Button>}
                        {!this.props.finished &&
                        <Button icon="stop">Stop</Button>}
                        <Button icon="refresh">Sync</Button>
                        {/*<Button icon="ninja">Tasks</Button>*/}
                    </div>
                </div>
            </Card>
        )
    }
}
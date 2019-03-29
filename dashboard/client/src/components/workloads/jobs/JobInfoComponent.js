import React from "react";
import JobService from "../../../services/JobService";
import {Button, Intent, Tag} from "@blueprintjs/core";
import {ComputeResourceCard} from "../../grid/compute-resource/ComputeResourceCard";
import NodeTag from "../../grid/nodes/NodeTag";
import {JobUtils} from "./JobUtils";
import ClusterTag from "../../grid/clusters/ClusterTag";
import WorkerCard from "../../grid/workers/WorkerCard";
import LoadingComponent from "../../ui/LoadingComponent";

export default class JobInfoComponent extends React.Component {

    constructor(props) {
        super(props);
        console.log(this.props);
        this.state = {
            jobId: this.props.match.params.jobId,
            job: undefined,
            stateIntent: undefined,
            loading: false
        }
    }

    setLoading = (loading) => {
        this.setState({
            loading: loading
        })
    };

    componentDidMount() {
        this.syncJob();
    }

    syncJob = () => {
        this.setLoading(true);
        JobService.getJobById(this.state.jobId).then(response => {
            this.setState({
                job: response.data,
                stateIntent: JobUtils.getStateIntent(response.data.state)
            });
            this.setLoading(false)
        }).catch(err => {
            this.setLoading(false)
        });
    };

    render() {
        if (!this.state.job) {
            return <LoadingComponent/>;
        }

        //sorting workers by id
        if (this.state.job.workers) {
            this.state.job.workers.sort((w1, w2) => {
                return w1.workerID - w2.workerID;
            });
        }

        //sort compute resource
        if (this.state.job.computeResources) {
            this.state.job.computeResources.sort((cr1, cr2) => {
                return cr1.index - cr2.index;
            });
        }

        return (
            <div>
                <div className="t2-jobs-header">
                    <h1 className="t2-page-heading">{this.state.job.jobName}</h1>
                    <div>
                        <Button icon="refresh" text="refresh" intent={Intent.NONE} onClick={this.syncJob}/>
                    </div>
                </div>
                <table className=" bp3-html-table bp3-html-table-striped bp3-html-table-condensed"
                       width="100%">
                    <tbody>
                    <tr>
                        <td>
                            Job ID
                        </td>
                        <td>
                            {this.state.job.jobID}
                        </td>
                    </tr>
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
                            Start Time
                        </td>
                        <td>
                            {new Date(this.state.job.createdTime).toUTCString()}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            State
                        </td>
                        <td>
                            <Tag minimal={true} intent={this.state.stateIntent}>{this.state.job.state}</Tag>
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Compute Resources
                        </td>
                        <td>
                            {this.state.job.computeResources.map(cr => {
                                return <ComputeResourceCard cr={cr} index={cr.index} key={cr.index}/>
                            })}
                        </td>
                    </tr>
                    </tbody>
                </table>
                <h4>Workers</h4>
                {this.state.job.workers.map(worker => {
                    return <WorkerCard worker={worker} key={worker.workerID}/>
                })}
            </div>
        );
    }
}
import React from "react";
import WorkerCard from "./WorkerCard";
import "./WorkerComponent.css";
import {WorkerService} from "../../../services/WorkerService";

export default class WorkerComponents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            workers: []
        };
    }

    componentDidMount() {
        this.loadWorkers();
    }

    loadWorkers = () => {
        WorkerService.getAllWorkers().then(response => {
            this.setState({
                workers: response.data
            });
        })
    };

    render() {

        let nodeCards = this.state.workers.map(worker => {
            return <WorkerCard worker={worker} index={worker.job.jobID + worker.workerID}/>
        });

        return (
            <div>
                <h1 className="t2-page-heading">Workers</h1>
                <div className="t2-nodes-container">
                    {nodeCards}
                </div>
            </div>
        );
    }
}
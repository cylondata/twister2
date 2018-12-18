import React from "react";
import JobCard from "./JobCard";
import "./JobComponent.css";
import JobService from "../../../services/JobService";
import {ControlGroup, HTMLSelect, InputGroup} from "@blueprintjs/core";

export default class JobsComponents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            jobs: []
        }
    }

    componentDidMount() {
        this.loadJobs();
    }

    loadJobs = () => {
        JobService.getAllJobs().then(jobsResponse => {
            this.setState({
                jobs: jobsResponse.data
            })
        });
    };

    render() {

        let jobCards = [];

        this.state.jobs.forEach(job => {
            jobCards.push(<JobCard key={job.jobID} job={job}/>)
        });

        return (
            <div>
                <h1 className="t2-page-heading">Jobs</h1>
                <ControlGroup fill={true} className="t2-jobs-search">
                    <HTMLSelect options={["Any", "Completed"]}/>
                    <InputGroup placeholder="Find jobs..."/>
                </ControlGroup>
                <div className="t2-jobs-container">
                    {jobCards}
                </div>
            </div>
        );
    }
}
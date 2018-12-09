import React from "react";
import JobCard from "./JobCard";
import "./JobComponent.css";
import NewJobCard from "./NewJobCard";
import {JobService} from "../../../services/JobService";

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

        let nodeCards = [];
        let nodeCards2 = [];

        this.state.jobs.forEach(job => {
            if (job.state !== "COMPLETED") {
                nodeCards.push(<JobCard key={job.jobID} job={job}/>)
            } else {
                nodeCards2.push(<JobCard key={job.jobID} job={job}/>)
            }
        });
        // for (let i = 0; i < 3; i++) {
        //     nodeCards.push(<JobCard key={i}/>);
        //     nodeCards2.push(<JobCard key={i} finished={true}/>);
        // }

        return (
            <div>
                <h1 className="t2-page-heading">Jobs</h1>
                <h4>Running jobs</h4>
                <div className="t2-nodes-container">
                    {nodeCards}
                    <NewJobCard/>
                </div>
                <h4>Completed jobs</h4>
                <div className="t2-nodes-container">
                    {nodeCards2}
                    {nodeCards2.length === 0 && "Non of the jobs have completed yet!"}
                </div>
            </div>
        );
    }
}
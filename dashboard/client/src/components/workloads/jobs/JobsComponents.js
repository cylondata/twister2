import React from "react";
import JobCard from "./JobCard";
import "./JobComponent.css";
import JobService from "../../../services/JobService";
import {Button, ControlGroup, HTMLSelect, InputGroup, Intent} from "@blueprintjs/core";

export default class JobsComponents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            searchResults: {},
            searchKeyword: "",
            searchStates: [],
            currentResultsPage: 0
        };

        this.searchTimer = -1;
    }

    componentDidMount() {
        this.loadJobs();
    }

    loadJobs = () => {
        JobService.searchJobs(
            this.state.searchStates, this.state.searchKeyword, this.state.currentResultsPage
        ).then(jobsResponse => {
            console.log(jobsResponse);
            this.setState({
                searchResults: jobsResponse.data
            })
        });
    };

    invokeSearch = () => {
        clearTimeout(this.searchTimer);
        setTimeout(this.loadJobs, 500);
    };

    onKeywordChange = (event) => {
        this.setState({
            searchKeyword: event.target.value
        }, this.invokeSearch);
    };

    onJobStateChange = (event) => {
        let state = event.target.value;
        let searchStates = [];
        if (state !== "Any") {
            searchStates.push(state.toUpperCase());
        }
        this.setState({
            searchStates: searchStates
        }, this.loadJobs)
    };

    render() {

        let jobCards = [];

        (this.state.searchResults.content || []).forEach(job => {
            jobCards.push(<JobCard key={job.jobID} job={job}/>)
        });

        return (
            <div>
                <div className="t2-jobs-header">
                    <h1 className="t2-page-heading">Jobs</h1>
                    <div>
                        <Button icon="refresh" text="refresh" intent={Intent.NONE} onClick={this.loadJobs}/>
                    </div>
                </div>
                <ControlGroup fill={true} className="t2-jobs-search">
                    <HTMLSelect
                        onChange={this.onJobStateChange}
                        options={["Any", "Starting", "Started", "Completed", "Failed", "Killed"]}/>
                    <InputGroup placeholder="Find jobs..." onChange={this.onKeywordChange}/>
                </ControlGroup>
                <div className="t2-jobs-container">
                    {jobCards}
                </div>
            </div>
        );
    }
}
import React from "react";
import JobCard from "./JobCard";
import "./JobComponent.css";
import NewJobCard from "./NewJobCard";

export default class JobsComponents extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {

        let nodeCards = [];
        let nodeCards2 = [];
        for (let i = 0; i < 3; i++) {
            nodeCards.push(<JobCard key={i}/>);
            nodeCards2.push(<JobCard key={i} finished={true}/>);
        }

        return (
            <div>
                <h1 className="t2-page-heading">Jobs</h1>
                <h4>Running jobs</h4>
                <div className="t2-nodes-container">
                    {nodeCards}
                    <NewJobCard/>
                </div>
                <h4>Finished jobs</h4>
                <div className="t2-nodes-container">
                    {nodeCards2}
                </div>
            </div>
        );
    }
}
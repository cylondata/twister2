import React from "react";
import WorkerCard from "./WorkerCard";
import "./WorkerComponent.css";

export default class WorkerComponents extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {

        let nodeCards = [];
        for (let i = 0; i < 5; i++) {
            nodeCards.push(<WorkerCard key={i}/>);
        }

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
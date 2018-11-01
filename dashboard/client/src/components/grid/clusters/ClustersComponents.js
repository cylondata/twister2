import React from "react";
import ClusterCard from "./ClusterCard";
import "./ClusterComponent.css";
import NewClusterCard from "./NewClusterCard";

export default class ClustersComponents extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {

        let nodeCards = [];
        for (let i = 0; i < 5; i++) {
            nodeCards.push(<ClusterCard key={i}/>);
        }

        return (
            <div>
                <h1 className="t2-page-heading">Clusters</h1>
                <div className="t2-nodes-container">
                    {nodeCards}
                    <NewClusterCard/>
                </div>
            </div>
        );
    }
}
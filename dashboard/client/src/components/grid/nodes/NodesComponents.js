import React from "react";
import NodeCard from "./NodeCard";
import "./NodeComponent.css";
import NewNodeCard from "./NewNodeCard";

export default class NodesComponents extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {

        let nodeCards = [];
        for (let i = 0; i < 10; i++) {
            nodeCards.push(<NodeCard key={i}/>);
        }

        return (
            <div>
                <h1 className="t2-page-heading">Nodes</h1>
                <div className="t2-nodes-container">
                    {nodeCards}
                    <NewNodeCard/>
                </div>
            </div>
        );
    }
}
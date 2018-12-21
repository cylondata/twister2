import React from "react";
import NodeCard from "./NodeCard";
import "./NodeComponent.css";
import NewNodeCard from "./NewNodeCard";
import {NodeService} from "../../../services/NodeService";

export default class NodesComponents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            nodes: []
        };
    }

    componentDidMount() {
        this.loadNodes();
    }

    loadNodes() {
        NodeService.loadAllNodes().then(respone => {
            this.setState({
                nodes: respone.data
            });
        });
    }

    render() {

        let nodeCards = this.state.nodes.map((n, index) => {
            return <NodeCard node={n} index={index}/>
        });

        return (
            <div>
                <h1 className="t2-page-heading">Nodes</h1>
                <div className="t2-nodes-container">
                    {nodeCards}
                    {/*<NewNodeCard/>*/}
                </div>
            </div>
        );
    }
}
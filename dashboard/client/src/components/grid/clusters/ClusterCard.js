import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./ClusterCard.css";
import NodeTag from "../nodes/NodeTag";

export default class ClusterCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            cluster: this.props.cluster
        };
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="layout-sorted-clusters" iconSize={40} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        {this.state.cluster.name}
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Name
                            </td>
                            <td>
                                {this.state.cluster.name}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Description
                            </td>
                            <td>
                                {this.state.cluster.description}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Nodes
                            </td>
                            <td>
                                {this.state.cluster.nodes.map(node => {
                                    return <NodeTag node={node}/>
                                })}
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    {/*<div className="tw-node-actions">*/}
                        {/*<Button icon="offline">Detach</Button>*/}
                        {/*<Button icon="refresh">Sync</Button>*/}
                    {/*</div>*/}
                </div>
            </Card>
        )
    }
}
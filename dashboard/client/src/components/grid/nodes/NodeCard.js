import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./NodeCard.css";
import ClusterTag from "../clusters/ClusterTag";

export default class NodeCard extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            node: this.props.node
        }
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="desktop" iconSize={40} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Data center
                            </td>
                            <td>
                                {this.state.node.dataCenter}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Rack
                            </td>
                            <td>
                                {this.state.node.rack}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                IP
                            </td>
                            <td>
                                {this.state.node.ip}
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Cluster
                            </td>
                            <td>
                                <ClusterTag cluster={this.state.node.cluster}/>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    {/*<div className="tw-node-actions">*/}
                        {/*<Button icon="offline">Detach</Button>*/}
                        {/*<Button icon="refresh">Sync</Button>*/}
                        {/*<Button icon="ninja">Workers</Button>*/}
                    {/*</div>*/}
                </div>
            </Card>
        )
    }
}
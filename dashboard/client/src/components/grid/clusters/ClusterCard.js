import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./ClusterCard.css";

export default class ClusterCard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="layout-sorted-clusters" iconSize={50} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        Cluster 1
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Name
                            </td>
                            <td>
                                My Twister Cluster
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Description
                            </td>
                            <td>
                                Twister2 cluster to run bank fraud detection
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Nodes
                            </td>
                            <td>
                                <Button text="Node 1" minimal={true} icon={"desktop"} small={true}/>
                                <Button text="Node 2" minimal={true} icon={"desktop"} small={true}/>
                                <Button text="Node 3" minimal={true} icon={"desktop"} small={true}/>
                                <Button text="Node 4" minimal={true} icon={"desktop"} small={true}/>
                                <Button text="Node 5" minimal={true} icon={"desktop"} small={true}/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                State
                            </td>
                            <td style={{color: "green"}}>
                                Running
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Heartbeat
                            </td>
                            <td>
                                {new Date().toTimeString()}
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    <div className="tw-node-actions">
                        <Button icon="offline">Detach</Button>
                        <Button icon="refresh">Sync</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
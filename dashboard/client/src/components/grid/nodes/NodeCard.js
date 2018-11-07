import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./NodeCard.css";

export default class NodeCard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="desktop" iconSize={50} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        Node 1
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                IP
                            </td>
                            <td>
                                10.0.0.1
                            </td>
                        </tr>
                        <tr>
                            <td>
                                OS
                            </td>
                            <td>
                                Ubuntu 16.04 / 4.15.0-36-generic
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Cluster
                            </td>
                            <td>
                                <Button text="Cluster X" minimal={true} icon={"layout-sorted-clusters"} small={true}/>
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
                        <Button icon="ninja">Workers</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./WorkerCard.css";
import {Link} from "react-router-dom";

export default class WorkerCard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO} className="tw-node-card">
                <div>
                    <Icon icon="ninja" iconSize={50} className="tw-node-icon"/>
                </div>
                <div className="tw-node-info-wrapper">
                    <h4>
                        Worker 1
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Name
                            </td>
                            <td>
                                My Twister Worker
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
                                Cluster
                            </td>
                            <td>
                                <Button text="Cluster 1" minimal={true} icon={"layout-sorted-clusters"} small={true}/>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Node
                            </td>
                            <td>
                                <Button text="Node 1" minimal={true} icon={"desktop"} small={true}/>
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
                        <Link to="/workers/worker1">
                            <Button icon="info-sign">Info</Button>
                        </Link>
                        <Button icon="refresh">Sync</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
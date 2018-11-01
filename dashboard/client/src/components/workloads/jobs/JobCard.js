import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./JobCard.css";

export default class JobCard extends React.Component {

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
                        Job 1
                    </h4>
                    <table className="bp3-html-table bp3-html-table-striped">
                        <tbody>
                        <tr>
                            <td>
                                Name
                            </td>
                            <td>
                                Fraud detection job
                            </td>
                        </tr>
                        <tr>
                            <td>
                                Description
                            </td>
                            <td>
                                Cash Transaction Monitoring. Identify cash transactions just below regulatory reporting
                                thresholds. Identify a series of cash disbursements by customer number that together
                                exceed regulatory reporting threshold. Billing. Identify unusually large number of
                                waived fee by branch or by employee.
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
                                Tasks
                            </td>
                            <td>
                                <Button text="Task 1" minimal={true} icon={"layers"} small={true}/>
                                <Button text="Task 2" minimal={true} icon={"layers"} small={true}/>
                                <Button text="Task 3" minimal={true} icon={"layers"} small={true}/>
                                <Button text="Task 4" minimal={true} icon={"layers"} small={true}/>
                                <Button text="Task 5" minimal={true} icon={"layers"} small={true}/>
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
                        {this.props.finished &&
                        <Button icon="play">Start</Button>}
                        {!this.props.finished &&
                        <Button icon="stop">Stop</Button>}
                        <Button icon="refresh">Sync</Button>
                        <Button icon="ninja">Tasks</Button>
                    </div>
                </div>
            </Card>
        )
    }
}
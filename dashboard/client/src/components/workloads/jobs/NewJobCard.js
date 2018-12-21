import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./JobCard.css";

export default class NewJobCard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Card interactive={true} elevation={Elevation.ZERO}>
                <div className="bp3-non-ideal-state">
                    <div className="bp3-non-ideal-state-visual">
                        <span className="bp3-icon bp3-icon-add"/>
                    </div>
                    <h4 className="bp3-heading">Submit a new Job</h4>
                    <div>Submit a new computing job for twister</div>
                    <Button rightIcon="arrow-right">Start</Button>
                </div>
            </Card>
        )
    }
}
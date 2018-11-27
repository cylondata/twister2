import React from "react";
import {Card, Elevation, Icon, Button} from "@blueprintjs/core";
import "./ClusterCard.css";

export default class NewClusterCard extends React.Component {

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
                    <h4 className="bp3-heading">Create a new Cluster</h4>
                    <div>Define a new cluster for twister2</div>
                    <Button rightIcon="arrow-right">Start</Button>
                </div>
            </Card>
        )
    }
}
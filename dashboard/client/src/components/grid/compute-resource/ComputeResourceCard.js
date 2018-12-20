//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
import React from "react";
import {Card, Elevation, Icon} from "@blueprintjs/core";
import "./ComputeResourceCard.css";

export class ComputeResourceCard extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Card elevation={Elevation.ZERO} className="compute-resource-card">
                <div>
                    <Icon icon="database"/> {this.props.cr.disk} GB
                </div>
                <div>
                    <Icon icon="widget-button"/> {this.props.cr.ram} GB
                </div>
                <div>
                    <Icon icon="calculator"/> {this.props.cr.cpu} Core{this.props.cr.cpu === 1 ? "" : "s"}
                </div>
                <div>
                    <Icon
                        icon="flow-linear"/> {this.props.cr.scalable ? "Scalable" : "Not Scalable"} [{this.props.cr.instances}]
                </div>
            </Card>
        );
    }
}

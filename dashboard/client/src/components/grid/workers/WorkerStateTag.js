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
import {Intent, Tag} from "@blueprintjs/core";
import "./WorkerStateTag.css";

export default class WorkerStateTag extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            state: this.props.state,
            stateIntent: this.getStateIntent(this.props.state)
        };
    }


    getStateIntent = (state) => {
        switch (state) {
            case "COMPLETED":
                return Intent.SUCCESS;
            case "KILLED":
                return Intent.WARNING;
            case "NOT_PINGING":
                return Intent.WARNING;
            case "FAILED":
                return Intent.DANGER;
            default:
                return Intent.NONE;
        }
    };


    componentWillReceiveProps(nextProps, nextContext) {
        this.setState({
            state: nextProps.state,
            stateIntent: this.getStateIntent(nextProps.state)
        });
    }

    render() {
        return (
            <div className="t2-worker-state-tag-wrapper">
                {
                    this.state.state !== "KILLED_BY_SCALE_DOWN" &&
                    <Tag minimal={true}
                         intent={this.state.stateIntent}>{this.state.state}</Tag>
                }
                {
                    this.state.state === "KILLED_BY_SCALE_DOWN" &&
                    <Tag minimal={true}
                         className="tw-worker-state-tag-killed-bsd"
                         title="KILLED_BY_SCALE_DOWN"
                         intent={this.state.stateIntent}>KILLED_BY_SD</Tag>
                }
            </div>
        );
    }
}
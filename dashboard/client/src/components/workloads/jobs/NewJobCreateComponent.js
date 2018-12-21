import React from "react";
import {FormGroup, InputGroup, Intent, TextArea, FileInput, Button} from "@blueprintjs/core";

export default class NewJobCreateComponent extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div>
                <h1 className="t2-page-heading">Submit New Job</h1>
                <div>
                    <FormGroup
                        label="Job Name"
                        labelFor="job name"
                        labelInfo="(required)">
                        <InputGroup id="job name" placeholder="My First Twister Job"/>
                    </FormGroup>
                    <FormGroup
                        label="Job Description">
                        <TextArea
                            style={{width: "100%"}}
                            large={true}
                            intent={Intent.PRIMARY}
                        />
                    </FormGroup>
                    <FormGroup
                        label="Cluster">
                        <div className="bp3-select">
                            <select>
                                <option>Cluster 1</option>
                                <option>Cluster 2</option>
                                <option>Cluster 3</option>
                            </select>
                        </div>
                    </FormGroup>
                    <FormGroup
                        label="Job File">
                        <FileInput disabled={false} text="Choose file..."/>
                    </FormGroup>
                    <Button text="Submit Job" intent={Intent.PRIMARY} rightIcon="arrow-right"/>

                </div>
            </div>
        );
    }
}
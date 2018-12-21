import React from "react";
import LOGO from "./gray_logo.png";
import "./LoadingComponent.css";

export default class LoadingComponent extends React.Component {
    render() {
        return (
            <div className="loading-component-wrapper">
                <img src={LOGO} className="animated infinite wobble delay-2s" width={this.props.width || 40}/>
            </div>
        );
    }
}
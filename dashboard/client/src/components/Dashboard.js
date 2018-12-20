import React from "react";
import {
    Navbar,
    Alignment,
    Button,
    Position,
    Menu,
    Popover,
    MenuItem,
    Divider,
    Tree,
    Classes, Toaster
} from "@blueprintjs/core";
import LOGO from "./half.svg";
import "./Dashboard.css";
import {Route, Switch} from "react-router-dom";
import NodesComponent from "./grid/nodes/NodesComponents";
import ClustersComponents from "./grid/clusters/ClustersComponents";
import JobsComponents from "./workloads/jobs/JobsComponents";
import DashboardHome from "./DashboardHome";
import NewJobCreateComponent from "./workloads/jobs/NewJobCreateComponent";
import WorkerComponents from "./grid/workers/WorkerComponents";
import WorkerInfoComponent from "./grid/workers/WorkerInfoComponent";

const MENU_NODES = 1;
const MENU_CLUSTERS = 2;
const MENU_WORKERS = 3;

const MENU_JOBS = 4;

const MENU_DOCS = 5;
const MENU_ABOUT = 6;

export const DashToaster = Toaster.create({
    position: Position.BOTTOM_RIGHT,
});

export default class Dashboard extends React.Component {

    constructor(props) {
        super(props);
    }

    onMenuClicked = (event) => {
        switch (event.id) {
            case MENU_NODES:
                this.props.history.push("/nodes");
                break;
            case MENU_CLUSTERS:
                this.props.history.push("/clusters");
                break;
            case MENU_JOBS:
                this.props.history.push("/jobs");
                break;
            case MENU_WORKERS:
                this.props.history.push("/workers");
                break;
            case MENU_DOCS:
                window.open('https://twister2.gitbook.io/twister2', '_blank');
                break;
            default:
                break;
        }
    };

    onHomeClicked = () => {
        this.props.history.push("/");
    };

    onCreateNewJobClicked = () => {
        this.props.history.push("/newjob");
    };

    render() {
        return (
            <div>
                <Navbar>
                    <Navbar.Group align={Alignment.LEFT}>
                        <Navbar.Heading>
                            <div className="logo-container">
                                <div>
                                    <img src={LOGO}/>
                                </div>
                                <div className="logo-text">
                                    TWISTER2
                                </div>
                            </div>
                        </Navbar.Heading>
                        <Navbar.Divider/>
                        <Button className="bp3-minimal" icon="home" text="Home"
                                onClick={this.onHomeClicked}/>
                        <Button className="bp3-minimal" icon="new-grid-item" text="Jobs"
                                onClick={() => {
                                    this.onMenuClicked({id: MENU_JOBS})
                                }}/>
                        <Popover content={
                            <Menu>
                                <Menu.Item icon="layout-sorted-clusters" text="Clusters" onClick={() => {
                                    this.onMenuClicked({id: MENU_CLUSTERS})
                                }}/>
                                <Menu.Item icon="desktop" text="Nodes" onClick={() => {
                                    this.onMenuClicked({id: MENU_NODES})
                                }}/>
                            </Menu>
                        } position={Position.BOTTOM_RIGHT}>
                            <Button className="bp3-minimal" icon="layout-auto" text="Grid"/>
                        </Popover>
                    </Navbar.Group>
                    <Navbar.Group align={Alignment.RIGHT}>
                        {/*<Button className="bp3-minimal" icon="plus" text="Submit"*/}
                        {/*onClick={this.onCreateNewJobClicked}/>*/}
                        <Popover content={
                            <Menu>
                                <Menu.Item icon="log-out" text="Logout"/>
                                <Menu.Item icon="cog" text="Settings"/>
                            </Menu>
                        } position={Position.BOTTOM_RIGHT}>
                            <Button className="bp3-minimal" icon="user" text=""/>
                        </Popover>
                    </Navbar.Group>
                </Navbar>
                <div className="dash-container">
                    <div className="dash-container-content">
                        <Switch>
                            <Route exact path='/' component={DashboardHome}/>
                            <Route exact path='/nodes' component={NodesComponent}/>
                            <Route exact path='/clusters' component={ClustersComponents}/>
                            <Route exact path='/jobs' component={JobsComponents}/>
                            <Route exact path='/newjob' component={NewJobCreateComponent}/>
                            <Route exact path='/workers' component={WorkerComponents}/>
                            <Route exact path='/workers/:workerId' component={WorkerInfoComponent}/>
                        </Switch>
                    </div>
                </div>
            </div>
        )
    }

}
import React from "react";
import {Doughnut} from 'react-chartjs-2';
import "./DashboardHome.css";
import {JobService} from "../services/JobService";
import {WorkerService} from "../services/WorkerService";


const JOB_STATE_COLOR_MAP = {
    "COMPLETED": "#2E7D32",
    "STARTING": "#26A69A",
    "STARTED": "#0288D1",
    "FAILED": "#e53935"
};

const WORKER_STATE_COLOR_MAP = {
    "COMPLETED": "#2E7D32",
    "STARTING": "#26A69A",
    "NOT_PINGING": "#F57C00",
    "RUNNING": "#2E7D32",
    "KILLED": "#424242",
    "FAILED": "#e53935"
};

export default class DashboardHome extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            jobChart: {
                labels: [],
                data: [],
                colors: []
            },
            workerChart: {
                labels: [],
                data: [],
                colors: []
            }
        };

        this.statUpdateInterval = 0;
    }

    updateJobStats = () => {
        JobService.getJobStats().then(response => {
            let labels = [];
            let data = [];
            let colors = [];
            response.data.forEach(stat => {
                labels.push(stat[0]);
                data.push(stat[1]);
                colors.push(JOB_STATE_COLOR_MAP[stat[0]]);
            });
            this.setState({
                jobChart: {
                    labels, data, colors
                }
            })
        });
    };

    updateWorkerStats = () => {
        WorkerService.getWorkerStats().then(response => {
            let labels = [];
            let data = [];
            let colors = [];
            response.data.forEach(stat => {
                labels.push(stat[0]);
                data.push(stat[1]);
                colors.push(WORKER_STATE_COLOR_MAP[stat[0]]);
            });
            this.setState({
                workerChart: {
                    labels, data, colors
                }
            })
        });
    };

    componentDidMount() {
        this.updateJobStats();
        this.updateWorkerStats();

        this.statUpdateInterval = setInterval(() => {
            this.updateJobStats();
            this.updateWorkerStats();
        }, 5000);
    }

    componentWillUnmount() {
        clearInterval(this.statUpdateInterval);
    }

    render() {
        let nodeCards = [];
        for (let i = 0; i < 5; i++) {
            //nodeCards.push(<JobCard key={i}/>);
        }

        return (
            <div>
                <h1 className="t2-page-heading">
                    Twister Dash
                </h1>
                <div className="t2-quick-view-charts">
                    <div className="t2-quick-view-chart-wrapper">
                        <h4 className="text-center">Workers</h4>
                        {this.state.workerChart.data.length > 0 ?
                            <Doughnut data={{
                                datasets: [{
                                    data: this.state.workerChart.data,
                                    backgroundColor: this.state.workerChart.colors
                                }],
                                labels: this.state.workerChart.labels
                            }}/> : <p className="text-center">
                                Couldn't find enough worker data to generate stats</p>}
                    </div>
                    <div className="t2-quick-view-chart-wrapper">
                        <h4 className="text-center">Jobs</h4>
                        {this.state.jobChart.data.length > 0 ?
                            <Doughnut data={{
                                datasets: [{
                                    data: this.state.jobChart.data,
                                    backgroundColor: this.state.jobChart.colors
                                }],
                                labels: this.state.jobChart.labels
                            }}/> : <p className="text-center">
                                Couldn't find enough job data to generate stats</p>}
                    </div>
                </div>
                <div>
                    <h4>Pinned Jobs</h4>
                    <div className="tw-dash-pinned-items-wrapper">
                        {nodeCards}
                        {nodeCards.length === 0 ? "No pinned jobs available!" : ""}
                    </div>
                </div>
            </div>
        );
    }
}
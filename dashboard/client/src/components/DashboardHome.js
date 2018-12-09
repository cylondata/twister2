import React from "react";
import {Doughnut} from 'react-chartjs-2';
import "./DashboardHome.css";
import JobCard from "./workloads/jobs/JobCard";

export default class DashboardHome extends React.Component {

    constructor(props) {
        super(props);
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
                        <Doughnut data={{
                            datasets: [{
                                data: [10, 20, 1],
                                backgroundColor: ["#2E7D32", "#FFA000", "#e53935"]
                            }],
                            labels: [
                                'Running',
                                'Paused',
                                'Error'
                            ]
                        }}/>
                    </div>
                    <div className="t2-quick-view-chart-wrapper">
                        <h4 className="text-center">Jobs</h4>
                        <Doughnut data={{
                            datasets: [{
                                data: [30, 60, 20],
                                backgroundColor: ["#2E7D32", "#FFA000", "#e53935"]
                            }],
                            labels: [
                                'Running',
                                'Detached',
                                'Error'
                            ]
                        }}/>
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
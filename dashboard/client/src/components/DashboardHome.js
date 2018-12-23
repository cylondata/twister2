import React from "react";
import {Doughnut} from 'react-chartjs-2';
import "./DashboardHome.css";
import JobService from "../services/JobService";
import {WorkerService} from "../services/WorkerService";
import JobCard from "./workloads/jobs/JobCard";
import {Button, Card, Icon} from "@blueprintjs/core";
import {StatsService} from "../services/StatsService";
import {Link} from "react-router-dom";
import LoadingWrapper from "./ui/LoadingWrapper";


const JOB_STATE_COLOR_MAP = {
    "COMPLETED": "#2E7D32",
    "STARTING": "#26A69A",
    "STARTED": "#0288D1",
    "FAILED": "#e53935",
    "KILLED": "#d9822b",
};

const WORKER_STATE_COLOR_MAP = {
    "COMPLETED": "#2E7D32",
    "STARTING": "#26A69A",
    "NOT_PINGING": "#F57C00",
    "RUNNING": "#2E7D32",
    "KILLED": "#d9822b",
    "FAILED": "#e53935"
};

const ELEMENT_STAT_PROPERTIES = {
    jobs: {
        title: "Jobs",
        icon: "new-grid-item",
        url: "/jobs",
        color: "#FF9800"
    },
    workers: {
        title: "Workers",
        icon: "ninja",
        url: "/jobs",
        color: "#004D40"
    },
    nodes: {
        title: "Nodes",
        icon: "desktop",
        url: "/nodes",
        color: "#2196F3"
    },
    clusters: {
        title: "Clusters",
        icon: "layout-sorted-clusters",
        url: "/clusters",
        color: "#6D4C41"
    },
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
            },
            activeJobs: [],
            inActiveJobs: [],
            elementStats: {
                jobs: 0,
                workers: 0,
                nodes: 0,
                clusters: 0
            },

            loadingJobStatsCharts: false,
            loadingWorkerStatsCharts: false,

            loadingActiveJobs: false,
            loadingInActiveJobs: false,
        };

        this.statUpdateInterval = 0;
    }

    setLoadingJobStatCharts = (loading) => {
        this.setState({
            loadingJobStatsCharts: loading
        });
    };

    setLoadingWorkerStatCharts = (loading) => {
        this.setState({
            loadingWorkerStatsCharts: loading
        });
    };

    setLoadingActiveJobs = (loading) => {
        this.setState({
            loadingActiveJobs: loading
        })
    };

    setLoadingInActiveJobs = (loading) => {
        this.setState({
            loadingInActiveJobs: loading
        })
    };

    updateActiveJobs = () => {
        this.setLoadingActiveJobs(true);
        JobService.getActiveJobs().then(response => {
            this.setState({
                activeJobs: response.data.content
            });
            this.setLoadingActiveJobs(false);
        }).catch(e => {
            this.setLoadingActiveJobs(false)
        });
    };

    updateInActiveJobs = () => {
        this.setLoadingInActiveJobs(true);
        JobService.getInActiveJobs().then(response => {
            this.setState({
                inActiveJobs: response.data.content
            });
            this.setLoadingInActiveJobs(false);
        }).catch(e => {
            this.setLoadingInActiveJobs(false);
        });
    };

    updateJobStats = () => {
        this.setLoadingJobStatCharts(true);
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
            });
            this.setLoadingJobStatCharts(false);
        }).catch(e => {
            this.setLoadingJobStatCharts(false);
        });
    };

    updateWorkerStats = () => {
        this.setLoadingWorkerStatCharts(true);
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
            });
            this.setLoadingWorkerStatCharts(false);
        }).catch(e => {
            this.setLoadingWorkerStatCharts(false)
        });
    };

    updateElementStats = () => {
        StatsService.getElementStats().then(response => {
            this.setState({
                elementStats: response.data
            })
        });
    };

    componentDidMount() {
        this.updateJobStats();
        this.updateWorkerStats();
        this.updateActiveJobs();
        this.updateInActiveJobs();
        this.updateElementStats();

        this.statUpdateInterval = setInterval(() => {
            this.updateJobStats();
            this.updateWorkerStats();
            this.updateActiveJobs();
            this.updateInActiveJobs();
            this.updateElementStats();
        }, 10000);
    }

    componentWillUnmount() {
        clearInterval(this.statUpdateInterval);
    }

    render() {
        let activeJobCards = this.state.activeJobs.map(job => {
            return <JobCard job={job} key={job.jobID + job.state}/>
        });

        let inActiveJobCards = this.state.inActiveJobs.map(job => {
            return <JobCard job={job} key={job.jobID + job.state}/>
        });

        let statCards = Object.keys(this.state.elementStats).map(stat => {
            return <Card className="t2-quick-view-info-card"
                         style={{backgroundColor: ELEMENT_STAT_PROPERTIES[stat].color}}>
                <div className="t2-quick-view-info-card-header">
                    <Icon icon={ELEMENT_STAT_PROPERTIES[stat].icon} iconSize={40}/>
                    <div className="t2-quick-view-info-card-header-count-wrapper">
                        <div className="t2-quick-view-info-card-header-count">
                            {this.state.elementStats[stat]}
                        </div>
                        <div
                            className="t2-quick-view-info-card-header-count-label">
                            {ELEMENT_STAT_PROPERTIES[stat].title}
                        </div>
                    </div>
                </div>

                <Link to={ELEMENT_STAT_PROPERTIES[stat].url}>
                    <Button minimal={true} icon="eye-open">
                        View Details
                    </Button>
                </Link>
            </Card>
        });


        return (
            <div>
                <div className="t2-quick-view-charts">
                    <div className="t2-quick-view-info-wrapper">
                        <h4>Summary</h4>
                        <div className="t2-quick-view-info-cards">
                            {statCards}
                        </div>
                    </div>
                    <div className="t2-quick-view-chart-wrapper">
                        <h4 className="text-center">Workers</h4>
                        <LoadingWrapper loading={this.state.loadingJobStatsCharts}>
                            {this.state.workerChart.data.length > 0 ?
                                <Doughnut data={{
                                    datasets: [{
                                        data: this.state.workerChart.data,
                                        backgroundColor: this.state.workerChart.colors
                                    }],
                                    labels: this.state.workerChart.labels
                                }}/> : <p className="text-center">
                                    Couldn't find enough worker data to generate stats</p>}
                        </LoadingWrapper>
                    </div>
                    <div className="t2-quick-view-chart-wrapper">
                        <h4 className="text-center">Jobs</h4>
                        <LoadingWrapper loading={this.state.loadingWorkerStatsCharts}>
                            {this.state.jobChart.data.length > 0 ?
                                <Doughnut data={{
                                    datasets: [{
                                        data: this.state.jobChart.data,
                                        backgroundColor: this.state.jobChart.colors
                                    }],
                                    labels: this.state.jobChart.labels
                                }}/> : <p className="text-center">
                                    Couldn't find enough job data to generate stats</p>}
                        </LoadingWrapper>
                    </div>
                </div>
                <div>
                    <h4>Active Jobs</h4>
                    <div className="">
                        <LoadingWrapper loading={this.state.loadingActiveJobs}>
                            {activeJobCards}
                            {activeJobCards.length === 0 ? "No active jobs available!" : ""}
                        </LoadingWrapper>
                    </div>

                    <h4>Inactive Jobs</h4>
                    <div className="">
                        <LoadingWrapper loading={this.state.loadingInActiveJobs}>
                            {inActiveJobCards}
                            {inActiveJobCards.length === 0 ? "No inactive jobs available!" : ""}
                        </LoadingWrapper>
                    </div>
                </div>
            </div>
        );
    }
}
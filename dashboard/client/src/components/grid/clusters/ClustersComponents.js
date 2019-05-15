import React from "react";
import ClusterCard from "./ClusterCard";
import "./ClusterComponent.css";
import NewClusterCard from "./NewClusterCard";
import ClusterService from "../../../services/ClusterService";

export default class ClustersComponents extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            clusters: []
        };
    }

    componentDidMount() {
        this.loadClusters();
    }

    loadClusters = () => {
        ClusterService.getAllClusters().then(response => {
            this.setState({
                clusters: response.data
            });
        });
    };

    render() {

        let nodeCards = this.state.clusters.map(cluster => {
           return <ClusterCard cluster={cluster}/>
        });

        return (
            <div>
                <h1 className="t2-page-heading">Clusters</h1>
                <div className="t2-nodes-container">
                    {nodeCards}
                    {/*<NewClusterCard/>*/}
                </div>
            </div>
        );
    }
}
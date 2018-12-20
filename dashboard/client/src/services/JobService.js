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
import axios from "axios";
import {RestService} from "./RestService";

export const JOB_STATE = {
    STARTING: "STARTING", STARTED: "STARTED", COMPLETED: "COMPLETED", FAILED: "FAILED"
};

export default class JobService {
    static getAllJobs() {
        return axios.get(
            RestService.buildURL("jobs")
        );
    }

    static getJobById(jobId) {
        return axios.get(
            RestService.buildURL("jobs/" + jobId)
        );
    }

    static getJobStats() {
        return axios.get(
            RestService.buildURL("jobs/stats/")
        );
    }

    static searchJobs(states = [], keyword = "", page = 0) {
        return axios.get(
            RestService.buildURL(`jobs/search?keyword=${keyword}&page=${page}&states=${states.join(",")}`, false)
        );
    }

    static getActiveJobs() {
        return JobService.searchJobs([JOB_STATE.STARTED, JOB_STATE.STARTING]);
    }

    static getInActiveJobs() {
        return JobService.searchJobs([JOB_STATE.COMPLETED, JOB_STATE.FAILED]);
    }
}
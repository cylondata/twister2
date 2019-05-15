import {RestService} from "./RestService";
import axios from "axios";

export class StatsService {

    static getElementStats() {
        return axios.get(RestService.buildURL("/stats/elements"));
    }
}
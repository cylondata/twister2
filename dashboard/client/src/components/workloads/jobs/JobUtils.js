import {Intent} from "@blueprintjs/core";

export class JobUtils {
    static getStateIntent = (state) => {
        switch (state) {
            case "STARTED":
                return Intent.PRIMARY;
            case "COMPLETED":
                return Intent.SUCCESS;
            case "FAILED":
                return Intent.DANGER;
            case "KILLED":
                return Intent.WARNING;
            default:
                return Intent.NONE;
        }
    };
}
import React from 'react';
import ReactDOM from 'react-dom';
import * as serviceWorker from './serviceWorker';
import Dashboard from "./components/Dashboard";
import "../node_modules/normalize.css/normalize.css";
import "../node_modules/@blueprintjs/icons/lib/css/blueprint-icons.css";
import "../node_modules/@blueprintjs/core/lib/css/blueprint.css";
import {Switch, Route, HashRouter} from 'react-router-dom'


setTimeout(() => {
    ReactDOM.render(
        <HashRouter>
            <Switch>
                <Route path='/' component={Dashboard}/>
            </Switch>
        </HashRouter>, document.getElementById('root'))
}, 1000);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();

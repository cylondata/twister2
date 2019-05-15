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
let fs = require('fs');
let http = require('http');
let crypto = require('crypto');

var algo = 'sha256';


function getSha(file, cb) {
    var shasum = crypto.createHash(algo);
    var s = fs.ReadStream(file);
    s.on('data', function (d) {
        shasum.update(d);
    });
    s.on('end', function () {
        var d = shasum.digest('hex');
        cb(d);
    });
}

function downloadAndProcess(artifact) {
    let parts = artifact.split(":");
    //console.log(parts)

    let fileName = artifact.replace(/[:\.]/g, "_") + ".jar";
    var file = fs.createWriteStream(fileName);
    let url = `http://repo.maven.apache.org/maven2/${parts[0].replace(/\./g, "/")}/${parts[1]}/${parts[2]}/${parts[1]}-${parts[2]}.jar`;
    //console.log(url);
    var request = http.get(url, function (response) {
        if (response.statusCode === 200) {
            var stream = response.pipe(file);
            stream.on('finish', function () {
                getSha(fileName, function (sha) {
                    console.log(`_maven_import(artifact = "${artifact}",licenses = ["notice"],sha256 = "${sha}",)`)
                });
            });
        } else {
            console.log("Failed ", url);
        }
    });
}

fs.readFile('./WORKSPACE', 'utf8', function (err, contents) {
    let regex = new RegExp("maven_jar\\(\\s+.+\\s+artifact\\s=\\s\\\"([a-zA-Z0-9_\\.:-]+)\"", "g");
    let groups = regex.exec(contents);
    while (groups) {
        downloadAndProcess(groups[1]);
        groups = regex.exec(contents);
    }
});


module.exports = downloadAndProcess;
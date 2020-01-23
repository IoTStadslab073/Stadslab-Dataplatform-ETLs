require("dotenv").config();
const FIFO = require('fast-fifo');
const request = require("sync-request");
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const parser = require('ngsi-parser');

function Extract() {
    console.log("[INFO] Extracting data for Spotten started.");
    // Load files from disk
    fs.readdir(path.join(__dirname, 'xlsx'), (err, files) => {
      if (err) console.error(err);
      files.forEach(file => {
          if (file == '.gitignore') return;
          const workbook = xlsx.readFile("./xlsx/" + file);
          // console.log(xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]));
          const jsonObj = xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
          Transform(jsonObj);
        });
    });
}

function Transform(array) {
  console.log("[INFO] Extracting data from file for Spotten done.");
  var q = new FIFO();
  console.log("[INFO] Transforming data for Spotten started.");
  array.forEach(item => {
      const _item = {
          id: "SpottenSensor" + item["Sensor ID"],
          type: "SpottenSensor",
          log_id: item["Log ID"],
          start: new Date(item["Start"]),
          end: new Date(item["End"]),
          sensor_id: item["Sensor ID"],
          sensor_name: item["Sensor Name"],
          sensor_type: item["Sensor Type"],
          sensor_latitude: item["Sensor Latitude"],
          sensor_longitude: item["Sensor Longitude"],
      };
    q.push({
        "actionType": "APPEND",
        "entities": [parser.parseEntity(_item)]
    });
  });
  console.log("[INFO] Preparing has been completed.");
  Load(q);
}

function Load(q) {
    console.log("[INFO] Loading data fragment for Spotten.NL started.");
    while (!q.isEmpty()) {
        const v = request('POST', process.env.ORION_URL, { retry: true, json: q.shift() });
        process.stdout.write(v.statusCode == 204 ? '.' : '!');
    }
    console.log("[INFO] Loading data fragment for Spotten.NL finished.");
}

function Main() {
    Extract();
}

Main();

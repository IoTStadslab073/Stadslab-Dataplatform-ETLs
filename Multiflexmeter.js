require("dotenv").config();
const request = require("sync-request");
const FIFO = require('fast-fifo');
const parser = require('ngsi-parser');

const ORION_URL = process.env.ORION_URL;
const MFM_DEVS = process.env.MULTIFLEXMETER_DEVICES;
const MFM_MSMS = process.env.MULTIFLEXMETER_MEASUREMENTS;
const MFM_IDENT = Buffer.from(process.env.MULTIFLEXMETER_USERNAME + ":" + process.env.MULTIFLEXMETER_PASSWORD).toString('base64');
const MFM_SVI = process.env.MULTIFLEXMETER_SENSOR_VALUE_INTERVAL;

function getDevices() {
  var denBoschDevices = [];
  const response = JSON.parse(request('GET', MFM_DEVS, {
  headers: {
    'Authorization': "Basic " + MFM_IDENT
  }}).getBody('utf-8'));
  response.forEach(item => {
    if (item.owner != "https://portal.multiflexmeter.net/api/v1/organisations/shertogenbosch/") return;
    denBoschDevices.push([item.identifier, item.payload_format.measurements]);
  });
  return denBoschDevices;
}

function Extract() {
  console.log("[INFO] Getting data fragment for Multiflexmeter started.");
  let devices = getDevices();
  let today = new Date();
  let yesterday = new Date(today.setDate(today.getDate() - 1)).toISOString();
  let data = [];

  devices.forEach(device => {
    // Multiflexmeter base URI / device id /
    let url = MFM_MSMS + device[0] + "/";
    device[1].forEach(measurement_type => {
      // Grab measurements
      const response = JSON.parse(request('GET', url +
        "?start=" + yesterday +
        "&end=" + new Date().toISOString() +
        "&measurement=" + measurement_type.name + "&resolution=" + MFM_SVI, {
          headers: {
            'Authorization': "Basic " + MFM_IDENT
          }
      }).getBody('utf-8'));
      // Queue the measurements
      // Make sure the device and type of data saved exists, if this is not the case, create it.
      if (data.findIndex(item => item[0] === device[0]) > -1) {
        data[data.findIndex(item => item[0] === device[0])].push([measurement_type.name, response]);
      } else {
        data.push([device[0], [measurement_type.name, response]]);
      }
    });
  });
  // Transform all data.
  console.log("[INFO] Getting data fragment for Multiflexmeter finished.");
  Transform(data);
}

function Transform(source) {
  console.log("[INFO] Transforming data fragment for Multiflexmeter started.");
  let q = new FIFO();
  console.log(source);
  source.forEach(item => {
    if (item[1][1].count == 0) return;
    for (let i = 0; i < item[1][1].count-1; i++) {
      const _item = {
          id: item[0],
          type: "Multiflexmeter",
          time: new Date(item[1][1].points[i].time),
          celcius: item[1][1].points[i].mean_value,
          waterDepth: item[2][1].points[i].mean_value,
      };
      q.push({
          "actionType": "APPEND",
          "entities": [parser.parseEntity(_item)]
      });
    }
  });
  console.log("[INFO] Transforming data fragment for Multiflexmeter finished.");
  Load(q);
}

function Load(q) {
  console.log("[INFO] Loading data fragment for Multiflexmeter started.");
  while (!q.isEmpty()) {
      const v = request('POST', process.env.ORION_URL, { retry: true, json: q.shift() });
      process.stdout.write(v.statusCode == 204 ? '.' : '!');
  }
  console.log("[INFO] Loading data fragment for Multiflexmeter finished.");
}

Extract();

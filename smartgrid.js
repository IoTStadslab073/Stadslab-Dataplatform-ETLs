require("dotenv").config();
const FIFO = require('fast-fifo');
const request = require("sync-request");
const BASE_URL = process.env.SG_BASE_URL;
const NODE_URL = BASE_URL + "v0/sensors/";
const ORION_URL = process.env.ORION_URL;

function Extract() {
  var sensors = [];
  console.log("[INFO] Extracting data for IoT Stadslab started.");
  console.log("[INFO] Requesting data from " + NODE_URL);
  const nodes = JSON.parse(request('GET', NODE_URL).getBody('utf-8'));
  nodes.forEach(element => {
    console.log("[INFO] Found IoT node: " + element);
    const sensor = JSON.parse(request('GET', NODE_URL + element).getBody('utf-8'));
    sensor.forEach(item => {
      console.log("[INFO] Found IoT sensor: " + item.sensor_id + " on node: " + item.sensor_name);
      sensors.push(item);
    })
  });
  return sensors;
}

function Transform(sensors) {
  var q = new FIFO();
  console.log("[INFO] Transforming data for IoT Stadslab started.");
  console.log("[INFO] Preparing the Store for the Orion Context Broker.");
  q.push({
    "actionType": "APPEND",
    "entities": [{
      "id": "SGNode1",
      "type": "SGNode",
      "address": {
        "type": "PostalAddress",
        "value": {
          "streetAddress": "Parallelweg 30",
          "addressRegion": "Noord Brabant",
          "addressLocality": "s-Hertogenbosch",
          "postalCode": "5223 AL"
        },
        "metadata": {}
      },
      "location": {
        "type": "geo:json",
        "value": {
          "type": "Point",
          "coordinates": [51.695873127756045, 5.293006896972656]
        }
      }
    }]
  });
  console.log("[INFO] Store has been prepared.");
  console.log("[INFO] Preparing the Shelves for the Orion Context Broker.");
  sensors.forEach(item => {
    q.push({
      "actionType": "APPEND",
      "entities": [
        {
          "id": "SGSensor" + item.sensor_id,
          "type": "SGSensor",
          "name": {
            "type": "Text", "value": "SGSensor " + item.sensor_id
          },
          "refSGNode": {
            "type": "Relationship",
            "value": "SGNode1"
          }
        }
      ]
    });
  });
  console.log("[INFO] Shelves have been prepared.");
  console.log("[INFO] Preparing the Products for the Orion Context Broker.");
  sensors.forEach(sensor => {
    sensor.sensor_data.forEach(data => {
      console.log(data.data);
      q.push({
        "actionType": "APPEND",
        "entities": [
          {
            "id": "SGMeasurement" + sensor.sensor_id,
            "type": "SGMeasurement",
            "name": {
              "type": "String",
              "value":"Sensor " + sensor.sensor_id
            },
            "dateIssued": {
              "type": "DateTime",
              "value": data.time,
            },
            "measurement": {
              "type": "Property",
              "value": data.data
            },
            "refSGSensor": {
              "type": "Relationship",
              "value": "SGSensor" + sensor.sensor_id
            }
          }
        ]
      });
    });
  });
  console.log("[INFO] Nodes have been prepared.");
  console.log("[INFO] Preparing has been completed.");
  return q;
}

async function Load(q) {
  console.log("[INFO] Loading data fragment for IoT Stadslab started.");
  while (!q.isEmpty()) {
    var i = 0;
    const v = request('POST', ORION_URL, { retry: true, json: q.shift() });
    process.stdout.write(v.statusCode == 204 ? '.' : '!');
    if ((++i / 450) % 2 === 0 || (++i / 450) % 2 === 1) {
      await sleep(5000)
    };
    i++;
  }
  console.log("[INFO] Loading data fragment for IoT Stadslab finished.");
}

function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms)
  })
}

function Main() {
  Load(Transform(Extract()));
}

Main();

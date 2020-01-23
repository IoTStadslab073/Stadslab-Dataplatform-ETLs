const cron = require("node-cron");
const express = require("express");
const fs = require("fs");

app = express();

cron.schedule("0 1 * * *", function() {
  if (shell.exec("node Multiflexmeter.js").code !== 0) {
    shell.exit(1);
  }
});

app.listen("3128");

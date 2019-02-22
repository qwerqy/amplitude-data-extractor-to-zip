const request = require("request");
const fs = require("fs");

// Your Amplitude API Key
const user = process.env.AMPLITUDE_API_KEY;
const pass = process.env.AMPLITUDE_SECRET_KEY;

// The period you want to extract the data from.
// TODO: remove hardcoded values before deployment
const startDate = "20190101T00";
const endDate = "20190102T00";

// Create authentication token
const auth = new Buffer(user + ":" + pass).toString("base64");

const options = {
  method: "GET",
  url: `https://amplitude.com/api/2/export`,
  qs: { start: startDate, end: endDate },
  headers: {
    Authorization: "Basic " + auth
  }
};

// Request the data and pipes the file into the directory
request(options).pipe(fs.createWriteStream("file.zip"));

const request = require("request");
const fs = require("fs");
const unzip = require("unzipper");
const zlib = require("zlib");
require("dotenv").config();

const mongo = require("mongodb").MongoClient;
const url = "mongodb://localhost:27017/";

class Scraper {
  constructor() {}

  login({ user, password }) {
    if (!user || !password) {
      console.log("Please complete the missing parameter.");
    } else if (!user && !password) {
      console.log("Please provide the API key & Secret key to proceed!");
    } else {
      this.user = user;
      this.password = password;
    }
  }

  extractData(start, end) {
    if (!start || !end) {
      console.log("Please complete the parameter");
    } else if (!start && !end) {
      console.log("Please enter the period of start and end date!");
    } else {
      console.log(`Period: ${start} to ${end}\nStarting to look for data.`);
      const auth = new Buffer.from(this.user + ":" + this.password).toString(
        "base64"
      );
      const options = {
        method: "GET",
        url: `https://amplitude.com/api/2/export`,
        qs: { start, end },
        headers: {
          Authorization: "Basic " + auth
        }
      };
      if (!fs.existsSync("./data")) {
        console.log("data folder not found, creating one now...");
        fs.mkdirSync("./data");
      }
      request(options)
        .pipe(unzip.Parse())
        .on("entry", function(entry) {
          let buffer = "";
          const gunzip = zlib.createGunzip();
          entry.pipe(gunzip);
          gunzip
            .on("data", data => {
              buffer += data.toString();
            })
            .on("end", () => {
              const improvisedBuffer = buffer
                .split("\n")
                .map(json => {
                  try {
                    return JSON.parse(json);
                  } catch (err) {
                    // console.log(`Broken JSON, skipping in a line in ${start}`);
                    return null;
                  }
                })
                .filter(e => !!e)
                .map(e => {
                  const transformedObject = {};
                  for (let key of Object.keys(e)) {
                    if (key.startsWith("$")) {
                      transformedObject[key.substring(1)] = e[key];
                    } else {
                      transformedObject[key] = e[key];
                    }
                  }
                  return Object.assign({}, transformedObject);
                });
              const selectedEvents = improvisedBuffer.filter(
                e =>
                  e.event_type === "Survey disqualified" ||
                  e.event_type === "Survey sent" ||
                  e.event_type === "Survey complete"
              );
              if (selectedEvents.length > 0) {
                // console.log(`${selectedEvents.length} events in ${start}`);
                // console.log(selectedEvents);
                mongo.connect(
                  url,
                  { useNewUrlParser: true },
                  async (err, client) => {
                    if (err) throw new Error(err);
                    console.log("Connected correctly to server");
                    const dataDb = client.db("data");
                    const rawDb = await dataDb.collection("rawData");
                    rawDb.insertMany(selectedEvents, (err, result) => {
                      if (err) {
                        console.log("Error: ", err);
                      } else {
                        console.log("Result: ", result);
                      }
                    });
                    client.close();
                  }
                );
              }
            });
        })
        .on("error", function(error) {
          console.log(`No data found, proceeding..`);
        });
    }
  }
}

const scraper = new Scraper();

scraper.login({
  user: process.env.API_KEY,
  password: process.env.SECRET_KEY
});

const period = [
  {
    start: "20180101T00",
    end: "20180131T23"
  },
  {
    start: "20180201T00",
    end: "20180228T23"
  },
  {
    start: "20180301T00",
    end: "20180331T23"
  },
  {
    start: "20180401T00",
    end: "20180430T23"
  },
  {
    start: "20180501T00",
    end: "20180531T23"
  },
  {
    start: "20180601T00",
    end: "20180630T23"
  },
  {
    start: "20180701T00",
    end: "20180731T23"
  },
  {
    start: "20180801T00",
    end: "20180831T23"
  },
  {
    start: "20180901T00",
    end: "20180930T23"
  },
  {
    start: "20181001T00",
    end: "20181031T23"
  },
  {
    start: "20181101T00",
    end: "20181130T23"
  },
  {
    start: "20181201T00",
    end: "20181231T23"
  },
  {
    start: "20190101T00",
    end: "20190131T23"
  }
];

Object.keys(period).map(i => {
  scraper.extractData(period[i].start, period[i].end);
});
//   function() {
//   console.log("done");
//   fs.readdir("./data", (err, files) => {
//     files.forEach(file => {
//       console.log(file);
//     });
//   });
// });

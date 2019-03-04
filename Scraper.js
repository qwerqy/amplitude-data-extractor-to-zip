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

  parseDataForResponseTime() {
    mongo.connect(url, { useNewUrlParser: true }, async (err, client) => {
      if (err) throw new Error(err);
      console.log("Connected correctly to server");
      const dataDb = client.db("data");
      const rawDb = await dataDb.collection("rawData");
      await dataDb.collection("parsedData");
      const pipeline = [
        {
          $group: {
            _id: {
              refcode: "$user_id",
              cid: "$event_properties.cid"
            },
            events: {
              $push: {
                event_type: "$event_type",
                event_time: {
                  $dateFromString: {
                    dateString: "$event_time"
                  }
                }
              }
            }
          }
        },
        {
          $project: {
            _id: 1.0,
            survey_sent: {
              $filter: {
                input: "$events",
                as: "event",
                cond: {
                  $eq: ["$$event.event_type", "Survey sent"]
                }
              }
            },
            survey_complete: {
              $filter: {
                input: "$events",
                as: "event",
                cond: {
                  $eq: ["$$event.event_type", "Survey complete"]
                }
              }
            },
            survey_disqualified: {
              $filter: {
                input: "$events",
                as: "event",
                cond: {
                  $eq: ["$$event.event_type", "Survey disqualified"]
                }
              }
            }
          }
        },
        {
          $project: {
            _id: 1.0,
            survey_sent: {
              $max: "$survey_sent.event_time"
            },
            survey_complete: {
              $max: "$survey_complete.event_time"
            },
            survey_disqualified: {
              $max: "$survey_disqualified.event_time"
            }
          }
        },
        {
          $project: {
            _id: 1.0,
            response_time: {
              $subtract: [
                {
                  $ifNull: ["$survey_complete", "$survey_disqualified"]
                },
                "$survey_sent"
              ]
            }
          }
        },
        {
          $out: "parsedData"
        }
      ];
      rawDb.aggregate(pipeline);
      client.close();
    });
  }
}

module.exports = Scraper;

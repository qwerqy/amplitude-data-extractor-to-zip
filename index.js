const request = require("request");
const fs = require("fs");
const unzip = require("unzipper");
const zlib = require("zlib");
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

  extractData(start, end, callback) {
    if (!start || !end) {
      console.log("Please complete the parameter");
    } else if (!start && !end) {
      console.log("Please enter the period of start and end date!");
    } else {
      console.log(`Period: ${start} to ${end}\nStarting to look for data.`);
      const auth = new Buffer(this.user + ":" + this.password).toString(
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
              // console.log(entry);
              const improvisedBuffer = JSON.stringify(
                buffer
                  .split("\n")
                  .map(json => {
                    try {
                      return JSON.parse(json);
                    } catch (err) {
                      return null;
                    }
                  })
                  .filter(e => !!e)
              );
              callback(null, improvisedBuffer);
              // entry.autodrain();
            });
        });
    }
  }
}

const scraper = new Scraper();

scraper.login({
  user: process.env.API_KEY,
  password: process.env.SECRET_KEY
});

scraper.extractData("20190101T00", "20190102T00", function(error, data) {
  if (error) throw new Error(error);
  else {
    fs.writeFile("file.json", data, "utf8", function() {
      console.log("Process completed.");
    });
  }
});

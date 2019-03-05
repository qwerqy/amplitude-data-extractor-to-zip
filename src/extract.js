const Scraper = require("../lib/Scraper");

const config = {
  user: process.env.API_KEY,
  password: process.env.SECRET_KEY,
};

const period = {
  start: "20181101T00",
  end: "20181130T00",
};

const scraper = new Scraper(config);

scraper.extractData(period);

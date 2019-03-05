const Scraper = require("../lib/Scraper");

const config = {
  user: process.env.API_KEY,
  password: process.env.SECRET_KEY,
};

const scraper = new Scraper(config);

scraper.parseData();

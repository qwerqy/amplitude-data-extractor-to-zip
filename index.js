const Scraper = require("./Scraper");

const scraper = new Scraper();

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

scraper.login({
  user: process.env.API_KEY,
  password: process.env.SECRET_KEY
});

// This is for extracting the data
// Object.keys(period).map(i => {
//   scraper.extractData(period[i].start, period[i].end);
// });

// This is for parsing the raw data
scraper.parseDataForResponseTime();

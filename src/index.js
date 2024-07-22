const csv = require("csv-parser");
const fs = require("fs");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const headerMap = [
  {id: "About Us", title: "About Us"},
  {id: "CAP/ Budget ", title: "CAP/ Budget "},
  {id: "City", title: "City"},
  {id: "Company Domain Name", title: "Company Domain Name"},
  {id: "Company name", title: "Company name"},
  {id: "Contact", title: "Contact"},
  {id: "Description", title: "Description"},
  {id: "ID/Status", title: "ID/Status"},
  {id: "Industry", title: "Industry"},
  {id: "Last Activity Date", title: "Last Activity Date"},
  {id: "Last Contacted", title: "Last Contacted"},
  {id: "Last Engagement Date", title: "Last Engagement Date"},
  {id: "Last Logged Call Date", title: "Last Logged Call Date"},
  {id: "Last Modified Date", title: "Last Modified Date"},
  {id: "Lead Status", title: "Lead Status"},
  {id: "Merged Company IDs", title: "Merged Company IDs"},
  {id: "NOTE WORTHY", title: "NOTE WORTHY"},
  {id: "Number of child companies", title: "Number of child companies"},
  {id: "Parent Company", title: "Parent Company"},
  {id: "Phone Ext-", title: "Phone Ext-"},
  {id: "Phone Number", title: "Phone Number"},
  {id: "Postal Code", title: "Postal Code"},
  {id: "State/Region", title: "State/Region"},
  {id: "Status (Relationship)", title: "Status (Relationship)"},
  {id: "Street Address", title: "Street Address"},
  {id: "Street Address 2", title: "Street Address 2"},
  {id: "Target Account", title: "Target Account"},
  {id: "Time First Seen", title: "Time First Seen"},
  {id: "Time Zone", title: "Time Zone"},
  {id: "Title", title: "Title"},
  {id: "Twitter Handle", title: "Twitter Handle"},
  {id: "Organization Id", title: "Organization Id"},
];

const csvFile = 'NEW SEMI REFINED WITH CONTACTS TAB FOCUS'


function replaceKeyInArray(arr, oldKey, newKey) {
  return arr.map(obj => {
    if (obj.hasOwnProperty(oldKey)) {
      obj[newKey] = obj[oldKey];
      delete obj[oldKey];
    }
  });
}

function generateNormalizedCSVFiles (chunkSize, csvName) {
  let part = 0;
  let records = [];
  let chunkArray = [];
  fs.createReadStream(`${csvName}.csv`)
      .pipe(csv())
      .on("data", (data) => {
        records.push(data);
        replaceKeyInArray(records, 'Record ID','Organization Id');
      })
      .on("end", () => {
        for (let i = 0; i < records.length; i += chunkSize) {
          let chunk = records.slice(i, i + chunkSize);
          chunkArray.push(chunk);
          part += 1;
          const csvWriter = createCsvWriter({
            path: `results/${csvName}-${part}.csv`,
            header: headerMap
          });
          csvWriter.writeRecords(chunk);
        }
      });
}

generateNormalizedCSVFiles(150, csvFile)
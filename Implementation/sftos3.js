const jsforce = require('jsforce');
const AWS = require('aws-sdk');
const { createObjectCsvWriter } = require('csv-writer');
const path = require('path');
const fs = require('fs');

// AWS S3 configuration

const Bucket = 'anradataset';
const Key = 'sftos3.csv';
const localFilePath = path.join(__dirname, Key);

const awsCredentials = require('../resource/awsConfig.json');



const s3 = new AWS.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});


// Salesforce credentials
const sfUsername = 'sairam.marupalla@dataxform.ai';
const sfPassword = '7668Sairam@K0Q8qqJD4jEydzn8LbKulQWNH';

// SOQL query
const soql = 'SELECT Id, Name FROM Account LIMIT 10';

// Initialize Salesforce connection
const conn = new jsforce.Connection({
  // You may need to adjust loginUrl if you are using sandbox or custom domain
  loginUrl: 'https://dataxform.my.salesforce.com'
});

// CSV writer setup
const csvWriter = createObjectCsvWriter({
  path: localFilePath,
  header: [
    { id: 'Id', title: 'ID' },
    { id: 'Name', title: 'NAME' }
  ]
});

// Connect to Salesforce
conn.login(sfUsername, sfPassword, function(err, userInfo) {
  if (err) {
    return console.error(err);
  }
  
  // Execute SOQL query
  conn.query(soql, function(err, result) {
    if (err) {
      return console.error(err);
    }

    // Write data to CSV
    csvWriter.writeRecords(result.records)
      .then(() => {
        console.log('CSV file was written successfully');

        // Read the file content to upload to S3
        const fileContent = fs.readFileSync(localFilePath);

        // Upload to S3
        const params = {
          Bucket,
          Key,
          Body: fileContent
        };

        s3.upload(params, function(s3Err, data) {
          if (s3Err) throw s3Err;
          console.log(`File uploaded successfully at ${data.Location}`);
          // Clean up local file after upload
          fs.unlinkSync(localFilePath);
        });
      })
      .catch(csvErr => console.error('Failed to write CSV:', csvErr));
  });
});

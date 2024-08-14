const express = require('express');
const AWS = require('aws-sdk');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const path = require('path');

// Load AWS credentials from the JSON file
const awsCredentials = require('../resource/awsConfig.json');

const app = express();
const s3 = new AWS.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region,
});

const RDS_HOST = 'database-2.civ74iupife5.us-east-1.rds.amazonaws.com'; // rds endpoint
const RDS_USER = 'admin';
const RDS_PASSWORD = 'dataxform';
const RDS_DATABASE = 'database-2';

const connection = mysql.createConnection({
  host: RDS_HOST,
  user: RDS_USER,
  password: RDS_PASSWORD,
  database: RDS_DATABASE,
});

app.use(bodyParser.json());

function getDataType(value) {
  if (typeof value === 'string') {
    return 'VARCHAR(255)';
  } else if (typeof value === 'number') {
    return Number.isInteger(value) ? 'INT' : 'FLOAT';
  } else if (typeof value === 'boolean') {
    return 'BOOLEAN';
  } else if (value instanceof Date) {
    return 'DATETIME';
  } else {
    return 'TEXT'; // Fallback for objects or arrays
  }
}

function generateCreateTableQuery(data) {
  const firstItem = data[0];
  let columns = Object.keys(firstItem).map(key => {
    const dataType = getDataType(firstItem[key]);
    return `\`${key}\` ${dataType}`;
  }).join(', ');

  return `CREATE TABLE IF NOT EXISTS my_table (id INT AUTO_INCREMENT PRIMARY KEY, ${columns});`;
}

function loadData(data) {
  const query = 'INSERT INTO my_table SET ?';
  return new Promise((resolve, reject) => {
    const promises = data.map(row => {
      return new Promise((resolve, reject) => {
        connection.query(query, row, (error, results) => {
          if (error) return reject(error);
          resolve(results);
        });
      });
    });
    Promise.all(promises)
      .then(results => resolve(results))
      .catch(error => reject(error));
  });
}

function downloadS3File(bucket, key) {
  const params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.getObject(params).promise();
}

app.post('/upload', async (req, res) => {
  const { bucketName, fileName } = req.body;

  if (!bucketName || !fileName) {
    return res.status(400).send('Bucket name and file name are required.');
  }

  try {
    console.log('Downloading file from S3...');
    const s3Data = await downloadS3File(bucketName, fileName);
    const fileContent = s3Data.Body.toString('utf-8');
    const data = JSON.parse(fileContent); // Assuming the file is in JSON format

    console.log('Generating CREATE TABLE query...');
    const createTableQuery = generateCreateTableQuery(data);
    console.log(createTableQuery);

    console.log('Creating table...');
    await new Promise((resolve, reject) => {
      connection.query(createTableQuery, (error, results) => {
        if (error) return reject(error);
        resolve(results);
      });
    });
    console.log('Table created successfully.');

    console.log('Loading data into database...');
    await loadData(data);
    console.log('Data loaded successfully.');

    res.send('Table created and data loaded successfully.');
  } catch (error) {
    console.error('Error:', error);
    res.status(500).send('An error occurred.');
  } finally {
    connection.end();
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

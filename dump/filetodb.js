const express = require('express');
const AWS = require('aws-sdk');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const stream = require('stream');

// Load AWS credentials from the JSON file
const awsCredentials = require('../resource/awsConfig.json');

const app = express();
const s3 = new AWS.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region,
});

const RDS_HOST = 'database-2.civ74iupife5.us-east-1.rds.amazonaws.com'; // RDS endpoint
const RDS_USER = 'admin';
const RDS_PASSWORD = 'dataxform';
const RDS_DATABASE = 'database2';

let connection;

function initializeConnection() {
  connection = mysql.createConnection({
    host: RDS_HOST,
    user: RDS_USER,
    password: RDS_PASSWORD,
    database: RDS_DATABASE,
    connectTimeout: 10000 // 10 seconds timeout
  });

  connection.connect((err) => {
    if (err) {
      console.error('Error connecting to the database:', err);
      process.exit(1);
    } else {
      console.log('Connected to the database.');
    }
  });
}

initializeConnection();

app.use(bodyParser.json());

function getDataType(value) {
   if (typeof value === 'number') {
    return Number.isInteger(value) ? 'INT' : 'FLOAT';
  } else if (typeof value === 'boolean') {
    return 'BOOLEAN';
  } else if (value instanceof Date) {
    return 'DATETIME';
    
  } 
  else if (typeof value === 'string') {
    return 'VARCHAR(255)';
  } 
  else {
    return 'TEXT'; // Fallback for objects or arrays
  }
}

function generateCreateTableQuery(data) {
  const firstItem = data[0];
  let columns = Object.keys(firstItem).map(key => {
    const dataType = getDataType(firstItem[key]);
    return `\`${key}\` ${dataType}`;
  }).join(', ');

  return `CREATE TABLE IF NOT EXISTS my_table (column_id INT AUTO_INCREMENT PRIMARY KEY, ${columns});`;
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

function parseCSV(data) {
  return new Promise((resolve, reject) => {
    const results = [];
    const readableStream = new stream.Readable();
    readableStream.push(data);
    readableStream.push(null);

    readableStream
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
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

    // Debug: Print the file content
    console.log('File content:', fileContent);

    console.log('Parsing CSV data...');
    const data = await parseCSV(fileContent);

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
  }
});

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
const express = require('express');
const AWS = require('aws-sdk');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const stream = require('stream');
const moment = require('moment');

const awsCredentials = require('../resource/awsConfig.json');

const app = express();
const s3 = new AWS.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region
});

const RDS_HOST = awsCredentials.RDS_HOST;
const RDS_USER = awsCredentials.RDS_USER;
const RDS_PASSWORD = awsCredentials.RDS_PASSWORD;
const RDS_DATABASE = awsCredentials.RDS_DATABASE;

let connection;

function initializeConnection() {
  connection = mysql.createConnection({
    host: RDS_HOST,
    user: RDS_USER,
    password: RDS_PASSWORD,
    database: RDS_DATABASE,
    connectTimeout: 10000 
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
  value = value.trim();

  if (!isNaN(parseFloat(value)) && isFinite(value)) {
    if (value.includes('.')) {
      return 'DECIMAL(10,2)'; 
    } else {
      return 'INT';
    }
  } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
    return 'BOOLEAN';
  } else if (!isNaN(Date.parse(value))) {
    return 'DATETIME';
  } else if (value.length > 255) {
    return 'TEXT';
  } else {
    return 'VARCHAR(255)';
  }
}

function detectColumnTypes(data) {
    console.log(data)
  const columnTypes = {};
  data.forEach(row => {
    Object.keys(row).forEach(key => {
      if (!columnTypes[key]) {
        columnTypes[key] = getDataType(row[key]);
      }
    });
  });
  return columnTypes;
}

function generateCreateTableQuery(data, originalFileName) {
  return new Promise((resolve, reject) => {
    console.log('*********************')
    console.log(originalFileName)
    
    let tableName = originalFileName.replace(/\W+/g, '_'); 
    console.log(tableName)
    console.log('*********************')
    const columnTypes = detectColumnTypes(data);
    let columns = Object.keys(columnTypes).map(key => `\`${key}\` ${columnTypes[key]}`).join(', ');

    
    connection.query(`SHOW TABLES LIKE '${tableName}'`, (error, results) => {
      if (error) return reject(error);
      if (results.length > 0) {
        tableName += `_${moment().format('YYYYMMDDHHmmss')}`;
      }

      resolve(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (column_id INT AUTO_INCREMENT PRIMARY KEY, ${columns});`);
    });
  });
}

function loadData(data, tableName) {
  const query = `INSERT INTO ${tableName} SET ?`;
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
    Key: key
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
    const s3Data = await downloadS3File(bucketName, fileName);
    const fileContent = s3Data.Body.toString('utf-8');
    const data = await parseCSV(fileContent);

    const createTableQuery = await generateCreateTableQuery(data, fileName);
    console.log(createTableQuery);

    await new Promise((resolve, reject) => {
      connection.query(createTableQuery, (error, results) => {
        if (error) return reject(error);
        resolve(results);
      });
    });

    await loadData(data, fileName.replace(/\W+/g, '_'));
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


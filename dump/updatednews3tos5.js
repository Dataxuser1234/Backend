const express = require('express');
const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const stream = require('stream');
const moment = require('moment');
const fs = require('fs');
const path = require('path'); // Include the path module

const awsCredentials = JSON.parse(fs.readFileSync('../resource/awsConfig.json', 'utf-8'));

const app = express();
const s3 = new aws.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region
});

const pool = mysql.createPool({
  host: awsCredentials.RDS_HOST,
  user: awsCredentials.RDS_USER,
  password: awsCredentials.RDS_PASSWORD,
  database: awsCredentials.RDS_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

app.use(bodyParser.json());

function sanitizeTableName(fileName) {
    const baseName = path.basename(fileName, path.extname(fileName));
    return baseName.replace(/[^a-zA-Z0-9_]/g, '_');
}

function detectDataType(samples) {
    let isInt = true, isDecimal = true, isBoolean = true, isDate = true, isDateTime = true;
    let maxLength = 0;

    samples.forEach(value => {
        value = value.trim();
        maxLength = Math.max(maxLength, value.length);
        if (isInt && !/^-?\d+$/.test(value)) isInt = false;
        if (isDecimal && !/^-?\d*(\.\d+)?$/.test(value)) isDecimal = false;
        if (isBoolean && !['true', 'false', '1', '0', '', null].includes(value.toLowerCase())) isBoolean = false;
        if (isDate && isNaN(Date.parse(value))) isDate = false;
        if (isDateTime && isNaN(new Date(value).getTime())) isDateTime = false;
    });

    if (isBoolean) return 'BOOLEAN';
    if (isInt && maxLength < 12) return 'INT';
    if (isInt) return 'BIGINT';
    if (isDecimal) return 'DECIMAL(10,2)';
    if (isDate) return 'DATE';
    if (isDateTime) return 'DATETIME';
    if (maxLength <= 255) return 'VARCHAR(255)';
    return 'TEXT';
}

function detectColumnTypes(data) {
    const columnSamples = {};
    const columnTypes = {};
    data.forEach(row => {
        Object.entries(row).forEach(([key, value]) => {
            if (!columnSamples[key]) columnSamples[key] = [];
            if (columnSamples[key].length < 10) {
                columnSamples[key].push(value);
            }
        });
    });

    Object.keys(columnSamples).forEach(key => {
        columnTypes[key] = detectDataType(columnSamples[key]);
    });

    return columnTypes;
}

async function createTableIfNotExists(tableName, columns) {
    const columnDefinitions = Object.entries(columns)
        .map(([column, type]) => `\`${column.replace(/`/g, '``')}\` ${type}`)
        .join(', ');
    const createTableQuery = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columnDefinitions});`;
    console.log('Query ',createTableQuery)
    await pool.query(createTableQuery);
}

async function insertDataBatch(tableName, data) {
    if (data.length === 0) return;

    const columns = Object.keys(data[0]).map(key => `\`${key.replace(/`/g, '``')}\``).join(', ');
    const values = data.map(row => 
        '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')'
    ).join(', ');

    const query = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${values};`;
    await pool.query(query);
}

async function downloadS3File(bucket, key) {
    const { Body } = await s3.getObject({ Bucket: bucket, Key: key }).promise();
    return Body;
}

async function parseCSV(data) {
    return new Promise((resolve, reject) => {
        const results = [];
        const readableStream = new stream.PassThrough();
        readableStream.push(data);
        readableStream.push(null);

        readableStream.pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
}

app.post('/upload', async (req, res) => {
    const { bucketName, fileName } = req.body;
    if (!bucketName || !fileName) {
        return res.status(400).send('Bucket name and file name are required.');
    }

    try {
        const fileContent = await downloadS3File(bucketName, fileName);
        const data = await parseCSV(fileContent);
        if (data.length > 0) {
            const columnTypes = detectColumnTypes(data);
            const tableName = sanitizeTableName(fileName);
            await createTableIfNotExists(tableName, columnTypes);
            await insertDataBatch(tableName, data);
        }
        res.send('Data uploaded and inserted successfully.');
    } catch (error) {
        console.error('Error:', error);
        res.status(500).send('An error occurred during the upload process.');
    }
});

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});


/*
//working fine
const express = require('express');
const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const stream = require('stream');
const moment = require('moment');
const fs = require('fs');

const awsCredentials = JSON.parse(fs.readFileSync('../resource/awsConfig.json', 'utf-8'));

const app = express();
const s3 = new aws.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region
});

const pool = mysql.createPool({
  host: awsCredentials.RDS_HOST,
  user: awsCredentials.RDS_USER,
  password: awsCredentials.RDS_PASSWORD,
  database: awsCredentials.RDS_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

app.use(bodyParser.json());

function detectDataType(samples) {
    let isInt = true, isDecimal = true, isBoolean = true, isDate = true, isDateTime = true;
    let maxLength = 0;

    samples.forEach(value => {
        value = value.trim();
        maxLength = Math.max(maxLength, value.length);
        if (isInt && !/^-?\d+$/.test(value)) isInt = false;
        if (isDecimal && !/^-?\d*(\.\d+)?$/.test(value)) isDecimal = false;
        if (isBoolean && !['true', 'false', '1', '0', '', null].includes(value.toLowerCase())) isBoolean = false;
        if (isDate && isNaN(Date.parse(value))) isDate = false;
        if (isDateTime && isNaN(new Date(value).getTime())) isDateTime = false;
    });

    if (isBoolean) return 'BOOLEAN';
    if (isInt && maxLength < 12) return 'INT';
    if (isInt) return 'BIGINT';
    if (isDecimal) return 'DECIMAL(10,2)';
    if (isDate) return 'DATE';
    if (isDateTime) return 'DATETIME';
    if (maxLength <= 255) return 'VARCHAR(255)';
    return 'TEXT';
}

function detectColumnTypes(data) {
    const columnSamples = {};
    const columnTypes = {};
    data.forEach(row => {
        Object.entries(row).forEach(([key, value]) => {
            if (!columnSamples[key]) columnSamples[key] = [];
            if (columnSamples[key].length < 10) {  // Sample up to 10 values
                columnSamples[key].push(value);
            }
        });
    });

    Object.keys(columnSamples).forEach(key => {
        columnTypes[key] = detectDataType(columnSamples[key]);
    });

    return columnTypes;
}

async function createTableIfNotExists(tableName, columns) {
    const columnDefinitions = Object.entries(columns)
        .map(([column, type]) => `\`${column.replace(/`/g, '``')}\` ${type}`)
        .join(', ');
    const createTableQuery = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columnDefinitions});`;
    console.log(createTableQuery);
    await pool.query(createTableQuery);
}

async function insertDataBatch(tableName, data) {
    if (data.length === 0) return;

    const columns = Object.keys(data[0]).map(key => `\`${key.replace(/`/g, '``')}\``).join(', ');
    const values = data.map(row => 
        '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')'
    ).join(', ');

    const query = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${values};`;
    await pool.query(query);
}

async function downloadS3File(bucket, key) {
    const { Body } = await s3.getObject({ Bucket: bucket, Key: key }).promise();
    return Body;
}

async function parseCSV(data) {
    return new Promise((resolve, reject) => {
        const results = [];
        const readableStream = new stream.PassThrough();
        readableStream.push(data);
        readableStream.push(null);

        readableStream.pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
}

app.post('/upload', async (req, res) => {
    const { bucketName, fileName } = req.body;
    if (!bucketName || !fileName) {
        return res.status(400).send('Bucket name and file name are required.');
    }

    try {
        const fileContent = await downloadS3File(bucketName, fileName);
        const data = await parseCSV(fileContent);
        if (data.length > 0) {
            const columnTypes = detectColumnTypes(data);
            const tableName = `table_${moment().format('YYYYMMDDHHmmss')}`;
            await createTableIfNotExists(tableName, columnTypes);
            await insertDataBatch(tableName, data);
        }
        res.send('Data uploaded and inserted successfully.');
    } catch (error) {
        console.error('Error:', error);
        res.status(500).send('An error occurred during the upload process.');
    }
});

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
*/




/*

//working
import express from 'express';
import aws from 'aws-sdk';
import mysql from 'mysql2/promise';
import bodyParser from 'body-parser';
import csv from 'csv-parser';
import stream from 'stream';
import moment from 'moment';
import fs from 'fs';

const awsCredentials = JSON.parse(fs.readFileSync('../resource/awsConfig.json', 'utf-8'));

const app = express();
const s3 = new aws.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region
});

const pool = mysql.createPool({
  host: awsCredentials.RDS_HOST,
  user: awsCredentials.RDS_USER,
  password: awsCredentials.RDS_PASSWORD,
  database: awsCredentials.RDS_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

app.use(bodyParser.json());

async function createTableIfNotExists(tableName, columns) {
  const columnDefinitions = columns.map(column => `\`${column.replace(/`/g, '``')}\` TEXT`).join(', ');
  const createTableQuery = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columnDefinitions});`;
  console.log(createTableQuery)
  await pool.query(createTableQuery);
}

async function insertDataBatch(tableName, data) {
  if (data.length === 0) return;

  const columns = Object.keys(data[0]).map(key => `\`${key.replace(/`/g, '``')}\``).join(', ');
  const values = data.map(row => 
    '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')'
  ).join(', ');

  const query = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${values};`;
  await pool.query(query);
}

async function downloadS3File(bucket, key) {
  const { Body } = await s3.getObject({ Bucket: bucket, Key: key }).promise();
  return Body;
}

async function parseCSV(data) {
  return new Promise((resolve, reject) => {
    const results = [];
    const readableStream = new stream.PassThrough();
    readableStream.push(data);
    readableStream.push(null);

    readableStream.pipe(csv())
      .on('data', (row) => results.push(row))
      .on('end', () => resolve(results))
      .on('error', reject);
  });
}

app.post('/upload', async (req, res) => {
  const { bucketName, fileName } = req.body;

  if (!bucketName || !fileName) {
    return res.status(400).send('Bucket name and file name are required.');
  }

  try {
    const fileContent = await downloadS3File(bucketName, fileName);
    const data = await parseCSV(fileContent);
    const tableName = `table_${moment().format('YYYYMMDDHHmmss')}`;

    if (data.length > 0) {
      await createTableIfNotExists(tableName, Object.keys(data[0]));
      await insertDataBatch(tableName, data);
    }

    res.send('Data uploaded and inserted successfully.');
  } catch (error) {
    console.error('Error:', error);
    res.status(500).send('An error occurred during the upload process.');
  }
});

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

*/







/*import express from 'express';
import aws from 'aws-sdk';
import mysql from 'mysql2/promise';
import bodyParser from 'body-parser';
import csv from 'csv-parser';
import stream from 'stream';
import fs from 'fs';

// AWS and database credentials loaded securely
const awsCredentials = JSON.parse(fs.readFileSync('../resource/awsConfig.json', 'utf-8'));

const app = express();
const s3 = new aws.S3({
  accessKeyId: awsCredentials.accessKeyId,
  secretAccessKey: awsCredentials.secretAccessKey,
  region: awsCredentials.region
});

const RDS_HOST = awsCredentials.RDS_HOST;
const RDS_USER = awsCredentials.RDS_USER;
const RDS_PASSWORD = awsCredentials.RDS_PASSWORD;
const RDS_DATABASE = awsCredentials.RDS_DATABASE;

// Create a pool of connections
const pool = mysql.createPool({
  connectionLimit: 10,
  host: RDS_HOST,
    user: RDS_USER,
    password: RDS_PASSWORD,
    database: RDS_DATABASE,
    connectTimeout: 10000 
});

app.use(bodyParser.json());

async function createTableIfNotExists(tableName, columns) {
  const createTableQuery = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columns});`;
  await pool.query(createTableQuery);
}

async function insertDataBatch(tableName, data) {
  const keys = Object.keys(data[0]).join(', ');
  const values = data.map(row =>
    `(${Object.values(row).map(value => mysql.escape(value)).join(', ')})`
  ).join(', ');
  const query = `INSERT INTO \`${tableName}\` (${keys}) VALUES ${values};`;
  await pool.query(query);
}

app.post('/upload', async (req, res) => {
  const { bucketName, fileName } = req.body;

  if (!bucketName || !fileName) {
    return res.status(400).send('Bucket name and file name are required.');
  }

  try {
    const { Body } = await s3.getObject({ Bucket: bucketName, Key: fileName }).promise();
    const csvStream = stream.PassThrough();
    csvStream.end(Body);

    const tableName = `table_${Date.now()}`;
    const data = [];
    let columnsDefined = false;

    csvStream.pipe(csv())
      .on('headers', (headers) => {
        const columns = headers.map(header => `\`${header}\` TEXT`).join(', ');
        createTableIfNotExists(tableName, columns);
        columnsDefined = true;
      })
      .on('data', (row) => {
        if (columnsDefined) {
          data.push(row);
          if (data.length >= 500) { // Adjust the batch size based on your use case
            insertDataBatch(tableName, data.splice(0, 500));
          }
        }
      })
      .on('end', async () => {
        if (data.length > 0) {
          await insertDataBatch(tableName, data);
        }
        res.send('Data uploaded and inserted successfully.');
      })
      .on('error', (error) => {
        console.error('Error processing CSV:', error);
        res.status(500).send('Failed to process CSV.');
      });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).send('An error occurred while uploading the file.');
  }
});

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

*/





/*import express from 'express';
import aws from 'aws-sdk';
import mysql from 'mysql2';
import bodyParser from 'body-parser';
import csv from 'csv-parser';
import stream from 'stream';
import moment from 'moment';
import fs from 'fs';

const awsCredentials = JSON.parse(fs.readFileSync('../resource/awsConfig.json', 'utf-8'));

const app = express();
const s3 = new aws.S3({
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
    let tableName = originalFileName.replace(/\W+/g, '_');
    
    const columnTypes = detectColumnTypes(data);
    let columns = Object.keys(columnTypes).map(key => `\`${key}\` ${columnTypes[key]}`).join(', ');

    connection.query(`SHOW TABLES LIKE '${tableName}'`, (error, results) => {
      if (error) return reject(error);
      if (results.length > 0) {
        tableName += `_${moment().format('YYYYMMDDHHmmss')}`;
      }

      resolve(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columns});`);
    });
  });
}

function loadData(data, tableName) {
  const baseQuery = `INSERT INTO ${tableName} `;
  let keys = Object.keys(data[0]).map(key => `\`${key}\``).join(', ');
  let values = data.map(row => 
    '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')'
  ).join(', ');

  const query = `${baseQuery}(${keys}) VALUES ${values};`;

  return new Promise((resolve, reject) => {
    connection.query(query, (error, results) => {
      if (error) return reject(error);
      resolve(results);
    });
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

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

*/

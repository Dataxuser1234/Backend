const express = require('express');
const aws = require('aws-sdk');
const bodyParser = require('body-parser');
const multer = require('multer');
const fs = require('fs');
const jwt = require('jsonwebtoken');

const upload = multer({ dest: 'uploads/' });
const awsCredentials = require('../resource/awsConfig.json');

const app = express();
app.use(bodyParser.json());

const s3 = new aws.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});

exports.authenticateJWT = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) {
        return res.status(401).send({ error: 'Authorization header missing' });
    }

    const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
    if (!token) {
        return res.status(401).send({ error: 'Token missing in Authorization header' });
    }

    jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
        if (err) {
            return res.status(403).send({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};

exports.uploadFileToS3 = async (req, res) => {
    if (!req.file) {
        return res.status(400).send({ error: 'No file uploaded.' });
    }

    const bucketName = req.query.bucketName;
    const fileName = req.file.originalname;
    const filePath = req.file.path;
    const fileStream = fs.createReadStream(filePath);

    try {
        await s3.upload({
            Bucket: bucketName,
            Key: fileName,
            Body: fileStream
        }).promise();

        res.send({
            success: true,
            message: 'File uploaded successfully to S3.',
            details: {
                fileName: fileName,
                bucketName: bucketName
            }
        });
    } catch (error) {
        console.error('Error uploading file:', error);
        res.status(500).send({
            error: 'An error occurred during the file upload to S3.',
            details: error.message
        });
    } finally {
        fs.unlink(filePath, err => {
            if (err) console.error('Failed to delete local file:', err);
            else console.log("Temporary file deleted.");
        });
    }
};






// const express = require('express');
// const aws = require('aws-sdk');
// const mysql = require('mysql2/promise');
// const bodyParser = require('body-parser');
// const multer = require('multer');
// const csv = require('csv-parser');
// const stream = require('stream');
// const moment = require('moment');
// const fs = require('fs');
// const path = require('path');
// const jwt = require('jsonwebtoken');

// const upload = multer({ dest: 'uploads/' });
// const awsCredentials = require('../resource/awsConfig.json');

// const app = express();
// app.use(bodyParser.json());

// const s3 = new aws.S3({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// const pool = mysql.createPool({
//     host: awsCredentials.RDS_HOST,
//     user: awsCredentials.RDS_USER,
//     password: awsCredentials.RDS_PASSWORD,
//     database: awsCredentials.RDS_DATABASE_DESTINATION,
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0
// });

// exports.authenticateJWT = (req, res, next) => {
//     console.log('Request headers:', req.headers); 
//     const authHeader = req.headers['authorization']
//     if (!authHeader) {
//         console.log('Authorization header missing');
//         return res.status(401).send({ error: 'Authorization header missing' });
//     }
//     console.log('Header', authHeader);

//     const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;

//     if (!token) {
//         console.log('Token missing in Authorization header');
//         return res.status(401).send({ error: 'Token missing in Authorization header' });
//     }

//     jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
//         if (err) {
//             console.log('Token verification failed:', err.message);
//             return res.status(403).send({ error: 'Invalid token' });
//         }
//         req.user = user;
//         next();
//     });
// };



// function sanitizeTableName(fileName) {
//     return path.basename(fileName, path.extname(fileName)).replace(/[^a-zA-Z0-9_]/g, '_');
// }

// function detectDataType(samples) {
//     let isInt = true, isDecimal = true, isBoolean = true, isDate = true, isDateTime = true;
//     let maxLength = 0;
//     samples.forEach(value => {
//         value = value.trim();
//         maxLength = Math.max(maxLength, value.length);
//         if (!/^-?\d+$/.test(value)) isInt = false;
//         if (!/^-?\d*(\.\d+)?$/.test(value)) isDecimal = false;
//         if (!['true', 'false', '1', '0', '', null].includes(value.toLowerCase())) isBoolean = false;
//         if (isNaN(Date.parse(value))) isDate = false;
//         if (isNaN(new Date(value).getTime())) isDateTime = false;
//     });
//     return isBoolean ? 'BOOLEAN' : isInt ? 'INT' : isDecimal ? 'DECIMAL(10,2)' : isDate ? 'DATE' : isDateTime ? 'DATETIME' : 'VARCHAR(255)';
// }

// function detectColumnTypes(data) {
//     const columnSamples = {};
//     data.forEach(row => {
//         Object.entries(row).forEach(([key, value]) => {
//             columnSamples[key] = columnSamples[key] || [];
//             if (columnSamples[key].length < 10) columnSamples[key].push(value);
//         });
//     });
//     return Object.fromEntries(Object.entries(columnSamples).map(([key, samples]) => [key, detectDataType(samples)]));
// }

// async function createTableIfNotExists(tableName, columns) {
//     const columnDefinitions = Object.entries(columns).map(([column, type]) => `\`${column}\` ${type}`).join(', ');
//     const createTableQuery = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (${columnDefinitions});`;
//     await pool.query(createTableQuery);
//     return tableName;
// }

// async function insertDataBatch(tableName, data) {
//     const columns = Object.keys(data[0]).map(key => `\`${key}\``).join(', ');
//     const values = data.map(row => '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')').join(', ');
//     const query = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${values};`;
//     const result = await pool.query(query);
//     return result[0].affectedRows; 
// }

// async function downloadS3File(bucket, key) {
//     const { Body } = await s3.getObject({ Bucket: bucket, Key: key }).promise();
//     return Body;
// }

// async function parseCSV(data) {
//     return new Promise((resolve, reject) => {
//         const results = [];
//         const readableStream = new stream.PassThrough();
//         readableStream.push(data);
//         readableStream.push(null);
//         readableStream.pipe(csv()).on('data', (row) => results.push(row)).on('end', () => resolve(results)).on('error', reject);
//     });
// }

// exports.uploadFileLoadintoSQL = async (req, res) => {
//     if (!req.file) {
//         return res.status(400).send({ error: 'No file uploaded.' });
//     }

//     const bucketName = req.query.bucketName;
//     const fileName = req.file.originalname;
//     const filePath = req.file.path;
//     const fileStream = fs.createReadStream(filePath);

//     try {
//         await s3.upload({ Bucket: bucketName, Key: fileName, Body: fileStream }).promise();
//         const fileContent = await downloadS3File(bucketName, fileName);
//         const data = await parseCSV(fileContent);
//         if (data.length > 0) {
//             const columnTypes = detectColumnTypes(data);
//            const tableName = await createTableIfNotExists(sanitizeTableName(fileName), columnTypes);
//             const insertCount = await insertDataBatch(tableName, data);
//             res.send({
//                 success: true,
//                 message: 'File processed successfully.',
//                 details: {
//                     fileName: fileName,
//                     bucketName: bucketName,
//                     recordCountInFile: data.length,
//                     database: awsCredentials.RDS_DATABASE,
//                     tableName: tableName,
//                     recordsInserted: insertCount
//                 }
//             });
//         } else {
//             throw new Error('No data to process in the file.');
//         }
//     } catch (error) {
//         console.error('Error processing file:', error);
//         res.status(500).send({
//             error: 'An error occurred during the file processing.',
//             details: error.message
//         });
//     } finally {
//         fs.unlink(filePath, err => {
//             if (err) console.error('Failed to delete local file:', err);
//             else console.log("Temporary file deleted.");
//         });
//     }
// };





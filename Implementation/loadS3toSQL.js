const express = require('express');
const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const stream = require('stream');
const path = require('path');
const jwt = require('jsonwebtoken');

const awsCredentials = require('../resource/awsConfig.json');
const { totalmem } = require('os');

const app = express();
app.use(bodyParser.json());

const s3 = new aws.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});

const pool = mysql.createPool({
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE_DESTINATION,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
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
        readableStream.pipe(csv()).on('data', (row) => results.push(row)).on('end', () => resolve(results)).on('error', reject);
    });
}

async function insertDataBatch(tableName, data) {
    const batchSize = 5000; // Include this in Postman Request
    for (let i = 0; i < data.length; i += batchSize) {
        
        const batchData = data.slice(i, i + batchSize);
        const columns = Object.keys(batchData[0]).map(key => `\`${key}\``).join(', ');
        const values = batchData.map(row => '(' + Object.values(row).map(value => mysql.escape(value)).join(', ') + ')').join(', ');
        const query = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${values};`;
        try {
            const [result] = await pool.query(query);
            console.log(`Inserted ${result.affectedRows} rows`);
            
        } catch (err) {
            console.error('Failed to insert batch:', err);
            throw err; 
        }
        
    }
}

//async function uploadFileLoadintoSQL(req, res) {
exports.loadfroms3tosql = async (req, res) => {
    const { bucketName, fileName, tableName } = req.query;

    if (!bucketName || !fileName || !tableName) {
        return res.status(400).send({ error: 'Missing required query parameters: bucketName, fileName, or tableName.' });
    }

    try {
        const fileContent = await downloadS3File(bucketName, fileName);
        const data = await parseCSV(fileContent);
        if (data.length > 0) {
            await insertDataBatch(tableName, data);
            res.send({
                success: true,
                message: 'File processed successfully.',
                details: {
                    fileName: fileName,
                    bucketName: bucketName,
                    recordCountInFile: data.length,
                    database: awsCredentials.RDS_DATABASE_DESTINATION,
                    tableName: tableName
                }
            });
        } else {
            throw new Error('No data to process in the file.');
        }
    } catch (error) {
        console.error('Error processing file:', error);
        res.status(500).send({
            error: 'An error occurred during the file processing.',
            details: error.message
        });
    }
}

// app.post('/loadfroms3tosql', uploadFileLoadintoSQL);

// const port = process.env.PORT || 3014;
// app.listen(port, () => {
//     console.log(`Server is running on port ${port}`);
// });

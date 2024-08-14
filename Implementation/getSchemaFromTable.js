const express = require('express');
const aws = require('aws-sdk');
const mysql = require('mysql2/promise');
const fs = require('fs');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');

// AWS S3 Configuration
const awsCredentials = require('../resource/awsConfig.json');
const s3 = new aws.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});

// MySQL Configuration
const mysqlConfig = {
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE
};


exports.authenticateJWT = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) {
        return res.status(401).send({ error: 'Authorization header missing' });
    }

    const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
    if (!token) {
        return res.status(401).send({ error: 'Token missing in Authorization header' });
    }

    jwt.verify(token, 'YOUR_SECRET_KEY', (err, user) => {
        if (err) {
            return res.status(403).send({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};


async function generateTableSchema(tableName) {
    const connection = await mysql.createConnection(mysqlConfig);
    const query = `
        SELECT COLUMN_NAME, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        AND TABLE_SCHEMA = ?`;
    const [rows] = await connection.execute(query, [tableName, mysqlConfig.database]);
    await connection.end();
    
    const schema = rows.map(row => `${row.COLUMN_NAME} ${row.COLUMN_TYPE}`).join('\n');
    return schema;
}

async function uploadToS3(content, bucketName, key) {
    const params = {
        Bucket: bucketName,
        Key: key,
        Body: content
    };
    await s3.upload(params).promise();
    console.log(`File uploaded successfully to ${bucketName}/${key}`);
}

// //const app = express();
// app.use(bodyParser.json());



exports.getSchemaFromTable = async (req, res) => {
//app.post('/getSchema', async (req, res) => {
    const { tableName, bucketName, fileName } = req.query;
    if (!tableName || !bucketName || !fileName) {
        return res.status(400).send({ error: 'Missing parameters. Please provide tableName, bucketName, and fileName.' });
    }

    try {
        const tableSchema = await generateTableSchema(tableName);
        await uploadToS3(tableSchema, bucketName, fileName);
        res.send({
            success: true,
            message: 'Table schema uploaded successfully.',
            details: {
                tableName,
                bucketName,
                fileName
            }
        });
    } catch (error) {
        console.error('Error processing request:', error);
        res.status(500).send({ error: 'Failed to process the request.', details: error.message });
    }
}

// const PORT = process.env.PORT || 3009;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });











// const express = require('express');
// const aws = require('aws-sdk');
// const mysql = require('mysql2/promise');
// const fs = require('fs');
// const bodyParser = require('body-parser');

// // AWS S3 Configuration
// const awsCredentials = require('../resource/awsConfig.json');
// const s3 = new aws.S3({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// // MySQL Configuration
// const mysqlConfig = {
//     host: awsCredentials.RDS_HOST,
//     user: awsCredentials.RDS_USER,
//     password: awsCredentials.RDS_PASSWORD,
//     database: awsCredentials.RDS_DATABASE
// };

// async function generateCreateTableQuery(tableName) {
//     const connection = await mysql.createConnection(mysqlConfig);
//     const query = `SHOW CREATE TABLE ${tableName}`;
//     const [rows] = await connection.execute(query);
//     await connection.end();
//     return rows[0]['Create Table'];
// }

// async function uploadToS3(content, bucketName, key) {
//     const params = {
//         Bucket: bucketName,
//         Key: key,
//         Body: content
//     };
//     await s3.upload(params).promise();
//     console.log(`File uploaded successfully to ${bucketName}/${key}`);
// }

// const app = express();
// app.use(bodyParser.json());

// app.post('/upload-schema', async (req, res) => {
//     const { tableName, bucketName, fileName } = req.query;
//     if (!tableName || !bucketName || !fileName) {
//         return res.status(400).send({ error: 'Missing parameters. Please provide tableName, bucketName, and fileName.' });
//     }

//     try {
//         const createTableQuery = await generateCreateTableQuery(tableName);
//         await uploadToS3(createTableQuery, bucketName, fileName);
//         res.send({
//             success: true,
//             message: 'Create table query uploaded successfully.',
//             details: {
//                 tableName,
//                 bucketName,
//                 fileName
//             }
//         });
//     } catch (error) {
//         console.error('Error processing request:', error);
//         res.status(500).send({ error: 'Failed to process the request.', details: error.message });
//     }
// });

// const PORT = process.env.PORT || 3009;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });

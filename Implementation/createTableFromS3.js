const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const streamToString = require('stream-to-string');
const jwt = require('jsonwebtoken');

// AWS S3 Configuration
const awsCredentials = require('../resource/awsConfig.json');
const s3Client = new S3Client({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});

// MySQL Configuration
const mysqlConfig = {
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE_DESTINATION
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

    jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
        if (err) {
            return res.status(403).send({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};

async function downloadTableSchema(bucketName, key) {
    const command = new GetObjectCommand({
        Bucket: bucketName,
        Key: key
    });
    const response = await s3Client.send(command);
    const data = await streamToString(response.Body);
    return data;
}

function formatSchema(schema) {
    const lines = schema.trim().split('\n').map(line => line.trim());
    const formattedLines = lines.map((line, index) => {
        const parts = line.split(/\s+/);
        const columnName = parts.slice(0, -1).join(' '); // everything except the last part (data type)
        const dataType = parts[parts.length - 1]; // the last part is the data type
        
        // Wrap column names with spaces in backticks
        const formattedColumnName = `\`${columnName}\``;
        
        // Construct the formatted line
        const formattedLine = `${formattedColumnName} ${dataType}${index < lines.length - 1 ? ',' : ''}`;
        return formattedLine;
    });
    return formattedLines.join('\n');
}

async function createTable(schema, tableName) {
    const connection = await mysql.createConnection(mysqlConfig);
    const formattedSchema = formatSchema(schema);
    const createTableQuery = `CREATE TABLE ${tableName} (\n${formattedSchema}\n)`;
    await connection.query(createTableQuery);
    await connection.end();
    return 'Table created successfully.';
}

const app = express();
app.use(bodyParser.json());

//app.post('/createtablefroms3', async (req, res) => {
exports.createtablefroms3 = async (req, res) => {
    const { bucketName, fileName, tableName } = req.query;
    if (!bucketName || !fileName || !tableName) {
        return res.status(400).send({ error: 'Missing parameters. Please provide bucketName, fileName, and tableName.' });
    }

   
    const pathParts = bucketName.split('/');
    const baseBucketName = pathParts.shift(); 
    const folder = pathParts.join('/'); 

    const fileKey = folder ? `${folder}/${fileName}` : fileName; // Combine folder and file name

    try {
        const tableSchema = await downloadTableSchema(baseBucketName, fileKey);
        const message = await createTable(tableSchema, tableName);
        res.send({
            success: true,
            message,
            details: {
                bucketName: baseBucketName,
                fileKey,
                tableName
            }
        });
    } catch (error) {
        console.error('Error processing request:', error);
        res.status(500).send({ error: 'Failed to process the request.', details: error.message });
    }
}

// const PORT = process.env.PORT || 3010;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });










// const express = require('express');
// const aws = require('aws-sdk');
// const mysql = require('mysql2/promise');
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
//     database: awsCredentials.RDS_DATABASE_DESTINATION
// };

// async function downloadCreateTableQuery(bucketName, key) {
//     const params = {
//         Bucket: bucketName,
//         Key: key
//     };
//     const data = await s3.getObject(params).promise();
//     return data.Body.toString('utf-8');
// }

// async function createTable(query) {
//     const connection = await mysql.createConnection(mysqlConfig);
//     await connection.query(query);
//     await connection.end();
//     return 'Table created successfully.';
// }

// const app = express();
// app.use(bodyParser.json());

// app.post('/createtablefroms3', async (req, res) => {
//     const { bucketName, fileName } = req.query;
//     if (!bucketName || !fileName) {
//         return res.status(400).send({ error: 'Missing parameters. Please provide bucketName and fileName.' });
//     }

//     try {
//         const createTableQuery = await downloadCreateTableQuery(bucketName, fileName);
//         const message = await createTable(createTableQuery);
//         res.send({
//             success: true,
//             message,
//             details: {
//                 bucketName,
//                 fileName
//             }
//         });
//     } catch (error) {
//         console.error('Error processing request:', error);
//         res.status(500).send({ error: 'Failed to process the request.', details: error.message });
//     }
// });

// const PORT = process.env.PORT || 3010;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });
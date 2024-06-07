const AWS = require('aws-sdk');
const awsCredentials = require('../resource/awsConfig.json');
AWS.config.update(awsCredentials);
const { generateErrorResponse,generateSuccessResponse } = require('./errorResponse');
const jwt = require('jsonwebtoken');
const { secretKey } = require('./jsonwebtoken');


exports.authenticateJWT = (req, res, next) => {
    console.log('Request headers:', req.headers); 
    const authHeader = req.headers['authorization'];
    if (!authHeader) {
        console.log('Authorization header missing');
        return res.status(401).send({ error: 'Authorization header missing' });
    }
    console.log('Header', authHeader);

    const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;

    if (!token) {
        console.log('Token missing in Authorization header');
        return res.status(401).send({ error: 'Token missing in Authorization header' });
    }

    jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
        if (err) {
            console.log('Token verification failed:', err.message);
            return res.status(403).send({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};


exports.getStats = (req, res) => {
    // const databaseName = req.query.databaseName.trim();
    // const tableName = req.query.tableName.trim();
    const { databaseName, tableName } = req.query;

    let missing = [];
    if (!databaseName) missing.push('Database Name');
    if (!tableName) missing.push('Table Name');

    if (missing.length > 0) {
        return res.status(400).send(generateErrorResponse(missing));
    }

    const glue = new AWS.Glue();
    glue.getTable({ DatabaseName: databaseName, Name: tableName }, (err, data) => {
        if (err) {
            res.status(500).send({ error: "Error fetching table schema." });
        } else {
            const columnNames = data.Table.StorageDescriptor.Columns.map(column => column.Name);
            glue.getColumnStatisticsForTable({
                DatabaseName: databaseName,
                TableName: tableName,
                ColumnNames: columnNames
            }, (err, data) => {
                if (err) {
                    res.status(500).send({ error: "Error fetching column statistics." });
                } else {
                    //res.json({ databaseName, tableName, columnNames, columnStatistics: data.ColumnStatisticsList });
                    res.status(200).send(generateSuccessResponse({ databaseName, tableName, columnNames, columnStatistics: data.ColumnStatisticsList }));
                }
            });
        }
    });
};

const AWS = require('aws-sdk');
const awsCredentials = require('../resource/awsConfig.json');
AWS.config.update(awsCredentials);
const { generateErrorResponse,generateSuccessResponse } = require('./errorResponse');
const jwt = require('jsonwebtoken');
const { secretKey } = require('./jsonwebtoken');


exports.authenticateJWT = (req, res, next) => {
    console.log('Request headers:', req.headers); 
    const authHeader = req.headers['authorization']
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


exports.startColumnStats = (req, res) => {
    const glue = new AWS.Glue();
    const { databaseName, tableName } = req.query;
    

    let missing = [];
    if (!databaseName) missing.push('Database Name');
    if (!tableName) missing.push('Table Name');

    if (missing.length > 0) {
        return res.status(400).send(generateErrorResponse(missing));
    }

    const params = {
        CatalogId: awsCredentials.CatalogId, 
        DatabaseName: databaseName,
        TableName: tableName,
        Role: awsCredentials.Role
    };

    glue.startColumnStatisticsTaskRun(params, function(err, data) {
        if (err) {
            console.error("Error starting column statistics:", err);
            res.status(500).send({ error: "Failed to start column statistics task" });
        } else {
            res.status(200).send(generateSuccessResponse({ message: "Column statistics task started successfully", data }));
        }
    });
};

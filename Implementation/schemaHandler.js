const { GlueClient, GetTableCommand } = require('@aws-sdk/client-glue');
const awsCredentials = require('../resource/awsConfig.json');
const glueClient = new GlueClient({ credentials: awsCredentials,region: 'us-east-1' });
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


exports.getSchema = async (req, res) => {
    const { databaseName, tableName } = req.query;
    let missing = [];
    if (!databaseName) missing.push('Database Name');
    if (!tableName) missing.push('Table Name');

    if (missing.length > 0) {
        return res.status(400).send(generateErrorResponse(missing));
    }
    try {
        const { Table } = await glueClient.send(new GetTableCommand({ DatabaseName: databaseName, Name: tableName }));
        const schema = Table.StorageDescriptor.Columns.map(column => ({ name: column.Name, type: column.Type }));
        //res.json({ databaseName, tableName, schema });
        res.status(200).send(generateSuccessResponse({databaseName, tableName, schema}));
    } catch (error) {
        console.error('Error retrieving table schema:', error);
        res.status(500).send({ error: 'Failed to retrieve table schema: ' + error.message });
    }
};

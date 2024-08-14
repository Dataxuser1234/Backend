const express = require('express');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const awsCredentials = require('../resource/awsConfig.json');
const jwt = require('jsonwebtoken');


const app = express();
app.use(bodyParser.json());




// (Source Database)
const dbConfig1 = {
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// (Target Database)
const dbConfig2 = {
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE_DESTINATION,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
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


const pool1 = mysql.createPool(dbConfig1);
const pool2 = mysql.createPool(dbConfig2);

async function transferData(sourceTable, targetTable) {
    // connect to src database and get data
    const connection1 = await pool1.getConnection();
    try {
        const [rows] = await connection1.query(`SELECT * FROM ${sourceTable}`);
        connection1.release();

        // connect to dest database and insert data
        const connection2 = await pool2.getConnection();
        try {
            const values = rows.map(row => Object.values(row).map(value => mysql.escape(value)).join(', ')).join('), (');
            const insertQuery = `INSERT INTO ${targetTable} VALUES (${values})`;
            const [result] = await connection2.query(insertQuery);
            connection2.release();
            return result;
        } catch (error) {
            connection2.release();
            throw error;
        }
    } catch (error) {
        connection1.release();
        throw error;
    }
}


//app.post('/LoadSQLtoSQL', async (req, res) => {

exports.loadSQLtoSQL = async (req, res, next) => {
    const { sourceTable, targetTable } = req.query;
    if (!sourceTable || !targetTable) {
        return res.status(400).send({ error: 'Both sourceTable and targetTable are required.' });
    }

    try {
        const result = await transferData(sourceTable, targetTable);
        res.send({
            success: true,
            message: 'Data transferred successfully.',
            affectedRows: result.affectedRows
        });
    } catch (error) {
        console.error('Error transferring data:', error);
        res.status(500).send({ error: 'Failed to transfer data.', details: error.message });
    }
}


// const PORT = 3011;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });

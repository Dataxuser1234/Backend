const express = require('express');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');
const awsCredentials = require('../resource/awsConfig.json');

const app = express();
app.use(bodyParser.json());

const dbConfig = {
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE_DESTINATION,
    waitForConnections: true,
    connectionLimit: 20, 
    queueLimit: 0
};

const pool = mysql.createPool(dbConfig);

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

async function getTotalRecordsCount(tableName) {
    try {
        const connection = await pool.getConnection();
        const [result] = await connection.query(`SELECT COUNT(*) AS totalCount FROM ${tableName}`);
        connection.release();
        return result[0].totalCount;
    } catch (error) {
        console.error('Error fetching total records count:', error);
        throw error;
    }
}

async function getColumnDetails(tableName) {
    try {
        const connection = await pool.getConnection();
        const [columns] = await connection.query(`SHOW COLUMNS FROM ${tableName}`);
        connection.release();
        return columns;
    } catch (error) {
        console.error('Error fetching column details:', error);
        throw error;
    }
}

async function getColumnStatistics(tableName, totalRecordsCount, columns) {
    try {
        const connection = await pool.getConnection();
        const statsPromises = columns.map(column => {
            const columnName = column.Field;
            return connection.execute(`SELECT 
                                            COUNT(\`${columnName}\`) AS count, 
                                            MIN(\`${columnName}\`) AS min, 
                                            MAX(\`${columnName}\`) AS max, 
                                            AVG(\`${columnName}\`) AS avg,
                                            SUM(CASE WHEN \`${columnName}\` IS NULL OR \`${columnName}\` = '' THEN 1 ELSE 0 END) AS numberOfNulls
                                        FROM ${tableName}`);
        });

        const results = await Promise.all(statsPromises);
        connection.release();

        return columns.map((column, index) => {
            const stats = results[index][0][0]; // Get the first result of the execute return array
            const populatedCount = totalRecordsCount - parseInt(stats.numberOfNulls);
            const populatedPercentage = ((populatedCount / totalRecordsCount) * 100).toFixed(2) + '%';

            return {
                columnName: column.Field,
                dataType: column.Type,
                statistics: {
                    count: stats.count,
                    min: stats.min,
                    max: stats.max,
                    avg: stats.avg,
                    numberOfNulls: stats.numberOfNulls,
                    populatedCount: populatedCount,
                    populatedPercentage: populatedPercentage
                }
            };
        });
    } catch (error) {
        console.error('Error fetching column statistics:', error);
        throw error;
    }
}

//app.get('/sqlColumnStatistics', async (req, res) => {

exports.sqlColumnStatistics = async (req, res) => {
    const { tableName } = req.query;
    if (!tableName) {
        return res.status(400).send({ error: 'tableName parameter is required.' });
    }

    try {
        const totalRecordsCount = await getTotalRecordsCount(tableName);
        const columns = await getColumnDetails(tableName);
        const columnStatistics = await getColumnStatistics(tableName, totalRecordsCount, columns);

        res.send({
            status: 'Success',
            statusCode: 200,
            response: {
                databaseName: dbConfig.database,
                tableName: tableName,
                recordsCount: totalRecordsCount,
                columnNames: columns.map(col => col.Field),
                columnStatistics: columnStatistics
            }
        });
    } catch (error) {
        console.error('Error fetching column statistics:', error);
        res.status(500).send({ error: 'Failed to fetch column statistics.', details: error.message });
    }
}

// const PORT = 3013;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });






// const express = require('express');
// const mysql = require('mysql2/promise');
// const bodyParser = require('body-parser');
// const awsCredentials = require('../resource/awsConfig.json');

// const app = express();
// app.use(bodyParser.json());


// const dbConfig = {
//     host: awsCredentials.RDS_HOST,
//     user: awsCredentials.RDS_USER,
//     password: awsCredentials.RDS_PASSWORD,
//     database: awsCredentials.RDS_DATABASE_DESTINATION,
//     waitForConnections: true,
//     connectionLimit: 10,
//     queueLimit: 0
// };


// const pool = mysql.createPool(dbConfig);


// async function getTotalRecordsCount(tableName) {
//     try {
//         const connection = await pool.getConnection();
//         const [result] = await connection.query(`SELECT COUNT(*) AS totalCount FROM ${tableName}`);
//         connection.release();
//         return result[0].totalCount;
//     } catch (error) {
//         console.error('Error fetching total records count:', error);
//         throw error;
//     }
// }


// async function getColumnDetails(tableName) {
//     try {
//         const connection = await pool.getConnection();
//         const [columns] = await connection.query(`SHOW COLUMNS FROM ${tableName}`);
//         connection.release();
//         return columns;
//     } catch (error) {
//         console.error('Error fetching column details:', error);
//         throw error;
//     }
// }


// async function getColumnStatistics(tableName, totalRecordsCount, columns) {
//     try {
//         const connection = await pool.getConnection();

//         const statsPromises = columns.map(async column => {
//             const columnName = column.Field;

//             const [stats] = await connection.query(`SELECT 
//                                                         COUNT(\`${columnName}\`) AS count, 
//                                                         MIN(\`${columnName}\`) AS min, 
//                                                         MAX(\`${columnName}\`) AS max, 
//                                                         AVG(\`${columnName}\`) AS avg,
//                                                         SUM(CASE WHEN \`${columnName}\` IS NULL  THEN 1 ELSE 0 END) AS numberOfNulls
//                                                     FROM ${tableName}`);

//             const populatedCount = totalRecordsCount - parseInt(stats[0].numberOfNulls);

//             const populatedPercentage = ((populatedCount / totalRecordsCount) * 100).toFixed(2) + '%';

//             return {
//                 columnName: columnName,
//                 dataType: column.Type,
//                 statistics: {
//                     count: stats[0].count,
//                     min: stats[0].min,
//                     max: stats[0].max,
//                     avg: stats[0].avg,
//                     numberOfNulls: stats[0].numberOfNulls,
//                     populatedCount: populatedCount,
//                     populatedPercentage: populatedPercentage
//                 }
//             };
//         });

//         const statistics = await Promise.all(statsPromises);
//         connection.release();

//         return statistics;
//     } catch (error) {
//         console.error('Error fetching column statistics:', error);
//         throw error;
//     }
// }


// app.get('/sqlColumnStatistics', async (req, res) => {
//     const { tableName } = req.query;
//     if (!tableName) {
//         return res.status(400).send({ error: 'tableName parameter is required.' });
//     }

//     try {
//         const totalRecordsCount = await getTotalRecordsCount(tableName);
//         const columns = await getColumnDetails(tableName);
//         const columnStatistics = await getColumnStatistics(tableName, totalRecordsCount, columns);

//         res.send({
//             status: 'Success',
//             statusCode: 200,
//             response: {
//                 databaseName: dbConfig.database,
//                 tableName: tableName,
//                 recordsCount: totalRecordsCount,
//                 columnNames: columns.map(col => col.Field),
//                 columnStatistics: columnStatistics
//             }
//         });
//     } catch (error) {
//         console.error('Error fetching column statistics:', error);
//         res.status(500).send({ error: 'Failed to fetch column statistics.', details: error.message });
//     }
// });

// const PORT = 3013;
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });

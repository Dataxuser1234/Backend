const express = require('express');
const multer = require('multer');
const fs = require('fs');
const csv = require('fast-csv');
const mysql = require('mysql2/promise');


const upload = multer({ dest: 'uploads/' });
const awsCredentials = require('../resource/awsConfig.json');

const pool = mysql.createPool({
    host: awsCredentials.RDS_HOST,
    user: awsCredentials.RDS_USER,
    password: awsCredentials.RDS_PASSWORD,
    database: awsCredentials.RDS_DATABASE,
    waitForConnections: true,
    connectionLimit: 20,
    queueLimit: 0
});

const app = express();


app.post('/uploadcsvSQL', upload.single('datafile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).send('No file uploaded.');
    }
    const { tableName } = req.query;

    const filePath = req.file.path;
    //const tableName = 'Org_Test_Data_10K';  

    try {
      
        const stream = fs.createReadStream(filePath);
        const csvData = [];

        const csvStream = csv.parse({ headers: true })
            .on('error', error => console.error(error))
            .on('data', row => {
                csvData.push(row);
                if (csvData.length >= 1000) {
                    stream.pause();  
                    pool.query(`INSERT INTO ${tableName} SET ?`, [csvData])
                        .then(() => {
                            csvData.length = 0; 
                            stream.resume(); 
                        })
                        .catch(err => {
                            console.error('Insert error:', err);
                            stream.destroy();
                            throw err;
                        });
                }
            })
            .on('end', async () => {
               
                if (csvData.length > 0) {
                    await pool.query(`INSERT INTO ${tableName} SET ?`, [csvData]);
                }
                res.send('CSV file has been successfully processed.');
            });

        stream.pipe(csvStream);
    } catch (error) {
        console.error('Failed to process file:', error);
        res.status(500).send('Failed to process the file.');
    } finally {
        fs.unlink(filePath, err => {
            if (err) console.error('Failed to delete temporary file:', err);
        });
    }
});

const PORT = 3012;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

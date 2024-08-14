const express = require('express');
const { S3Client, GetObjectCommand, ListObjectsCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const app = express();
app.use(express.json());

const awsCredentials = require('../resource/awsConfig.json');
const s3Client = new S3Client({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});

app.post('/loadData', async (req, res) => {
    const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;

    const conn = new jsforce.Connection({
        loginUrl: salesforceLoginUrl
    });

    try {
        await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
        console.log('Successfully logged into Salesforce with API version:', conn.version);

        const listParams = {
            Bucket: bucketName,
            Prefix: folderPath
        };
        const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));

        for (const file of Contents) {
            console.log(`Processing file: ${file.Key}, Size: ${file.Size} bytes`);

            const getObjectParams = {
                Bucket: bucketName,
                Key: file.Key
            };
            const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
            const chunks = [];
            for await (const chunk of Body) {
                chunks.push(chunk);
            }
            const csvData = Buffer.concat(chunks);
            console.log(`Size of data to upload: ${csvData.length} bytes`);

            if (csvData.length > 150000000) {
                console.error('Data exceeds the Salesforce size limit.');
                
            } else {
                await uploadCSVToSalesforce(csvData.toString('utf8'), conn, salesforceObjectAPIName);
            }
        }
        res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' });
    } catch (error) {
        console.error('Failed to process files:', error);
        res.status(500).json({ message: 'Failed to process files', error: error.response ? error.response.data : error.toString() });
    }
});

const PORT = process.env.PORT || 3333;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
    const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
    try {
        const jobCreationResponse = await axios.post(apiURL, {
            object: salesforceObjectAPIName,
            operation: 'insert',
            contentType: 'CSV'
        }, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        const uploadURL = `${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`;
        await axios.put(uploadURL, csvData, {
            headers: {
                'Content-Type': 'text/csv',
                Authorization: `Bearer ${conn.accessToken}`
            }
        });

        const closeJobResponse = await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
            state: 'UploadComplete'
        }, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        console.log(`Job ${jobCreationResponse.data.id} closed with state: ${closeJobResponse.data.state}`);
    } catch (error) {
        console.error('Error uploading to Salesforce:', error);
        throw new Error(`Failed to upload CSV to Salesforce: ${JSON.stringify(error.response ? error.response.data : error)}`);
    }
}



// const express = require('express');
// const { S3Client, GetObjectCommand, ListObjectsCommand } = require('@aws-sdk/client-s3');
// const jsforce = require('jsforce');
// const axios = require('axios');
// const app = express();
// app.use(express.json());

// const awsCredentials = require('../resource/awsConfig.json');
// const s3Client = new S3Client({
//     region: awsCredentials.region,
//     credentials: {
//         accessKeyId: awsCredentials.accessKeyId,
//         secretAccessKey: awsCredentials.secretAccessKey
//     }
// });

// app.post('/loadData', async (req, res) => {
//     const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;

//     const conn = new jsforce.Connection({
//         loginUrl: salesforceLoginUrl
//     });

//     try {
//         await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
//         console.log('Successfully logged into Salesforce with API version:', conn.version);

//         const listParams = {
//             Bucket: bucketName,
//             Prefix: folderPath // Include folder path
//         };
//         const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));

//         for (const file of Contents) {
//             const getObjectParams = {
//                 Bucket: bucketName,
//                 Key: file.Key
//             };
//             const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
//             const csvData = await streamToString(Body);
//             console.log(`Size of data to upload: ${Buffer.byteLength(csvData, 'utf8')} bytes`);

//             // Proceed with the upload if size is within limits
//             if (Buffer.byteLength(csvData, 'utf8') <= 150000000) {
//                 await uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName);
//             } else {
//                 console.error('Data exceeds the Salesforce size limit.');
//                 // Optionally split the data or handle the error
//                 continue;
//             }


//             await uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName);
//         }
//         res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' });
//     } catch (error) {
//         console.error('Failed to process files:', error);
//         res.status(500).json({ message: 'Failed to process files', error: error.response ? error.response.data : error.toString() });
//     }
// });



// const PORT = process.env.PORT || 3333;
// app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// const streamToString = (stream) => {
//     const chunks = [];
//     return new Promise((resolve, reject) => {
//         stream.on('data', (chunk) => chunks.push(chunk));
//         stream.on('error', reject);
//         stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
//     });
// };

// async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
//     const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
//     try {
//         const jobCreationResponse = await axios.post(apiURL, {
//             object: salesforceObjectAPIName,
//             operation: 'insert',
//             contentType: 'CSV'
//         }, {
//             headers: {
//                 Authorization: `Bearer ${conn.accessToken}`,
//                 'Content-Type': 'application/json'
//             }
//         });

//         const uploadURL = `${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`;
//         await axios.put(uploadURL, csvData, {
//             headers: {
//                 'Content-Type': 'text/csv',
//                 Authorization: `Bearer ${conn.accessToken}`
//             }
//         });

//         const closeJobResponse = await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//             state: 'UploadComplete'
//         }, {
//             headers: {
//                 Authorization: `Bearer ${conn.accessToken}`,
//                 'Content-Type': 'application/json'
//             }
//         });

//         console.log(`Job ${jobCreationResponse.data.id} closed with state: ${closeJobResponse.data.state}`);
//     } catch (error) {
//         console.error('Error uploading to Salesforce:', error);
//         console.error(`Detailed error: ${JSON.stringify(error.response ? error.response.data : error)}`);
//         throw new Error(`Failed to upload CSV to Salesforce: ${JSON.stringify(error.response ? error.response.data : error)}`);
//     }
// }








/* 

const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const fs = require('fs');
const readline = require('readline');
const path = require('path');
const csvSplitStream = require('csv-split-stream');


const app = express();
app.use(express.json());

const awsCredentials = require('../resource/awsConfig.json');
const s3Client = new S3Client({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});

const streamToString = (stream) => {
    const chunks = [];
    return new Promise((resolve, reject) => {
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
};

app.post('/loadData', async (req, res) => {
    const { fileName, bucketName, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;

    const conn = new jsforce.Connection({
        loginUrl: salesforceLoginUrl
    });

    try {
        await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
        console.log('Successfully logged into Salesforce with API version:', conn.version);

        const getObjectParams = {
            Bucket: bucketName,
            Key: fileName
        };
        const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
        const csvData = await streamToString(Body);

        if (Buffer.byteLength(csvData, 'utf8') > 150000000) {
            await splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res);
        } else {
            await uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName, res);
        }
    } catch (error) {
        console.error('Failed to process files:', error);
        res.status(500).json({ message: 'Failed to process files', error: error.response ? error.response.data : error.toString() });
    }
});

const PORT = process.env.PORT || 3333;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

async function splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res) {
    const tempDir = './temp_csv_files';
    if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir);

    const tempCsvPath = path.join(tempDir, 'input.csv');
    fs.writeFileSync(tempCsvPath, csvData);

    try {
        const result = await csvSplitStream.split(
            fs.createReadStream(tempCsvPath),
            {
                lineLimit: 1000000
            },
            (index) => fs.createWriteStream(path.join(tempDir, `output-${index}.csv`))
        );

        console.log('CSV split into ' + result.totalChunks + ' files.');

        // Loop over the number of chunks instead of non-existent promises array
        for (let i = 0; i < result.totalChunks; i++) {
            const partPath = path.join(tempDir, `output-${i}.csv`);
            const partData = fs.readFileSync(partPath, 'utf8');
            await uploadCSVToSalesforce(partData, conn, salesforceObjectAPIName); // Upload logic
            fs.unlinkSync(partPath); // Delete the part file after uploading
        }

        fs.unlinkSync(tempCsvPath); // Delete the original large CSV file
        fs.rmdirSync(tempDir, { recursive: true }); // Remove the temporary directory
        res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' });
    } catch (error) {
        console.error('Error during CSV splitting process:', error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Failed to process files', error: error.toString() });
        }
    }
}



async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
    const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
    try {
        const jobCreationResponse = await axios.post(apiURL, {
            object: salesforceObjectAPIName,
            operation: 'insert',
            contentType: 'CSV'
        }, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        const uploadURL = `${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`;
        await axios.put(uploadURL, csvData, {
            headers: {
                'Content-Type': 'text/csv',
                Authorization: `Bearer ${conn.accessToken}`
            }
        });

        const closeJobResponse = await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
            state: 'UploadComplete'
        }, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        console.log(`Job ${jobCreationResponse.data.id} closed with state: ${closeJobResponse.data.state}`);
    } catch (error) {
        console.error('Error uploading to Salesforce:', error);
        throw new Error(`Failed to upload CSV to Salesforce: ${error.response ? error.response.data : error.toString()}`);
    }
}

    
    function countCSVRecords(csvData) {
        return csvData.split('\n').length - 1; // Subtract 1 for header line
    }
    
*/
 



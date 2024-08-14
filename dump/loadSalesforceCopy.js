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
    

 



/*



// Working



const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const fs = require('fs');
const csvSplitStream = require('csv-split-stream');

const path = require('path');

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
        version: '54.0',
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
            // Split and upload if CSV data is larger than 150 MB
            await splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res);
        } else {
            // Upload directly if under the limit
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


        for (let i = 0; i < result.totalChunks; i++) {
            const partPath = path.join(tempDir, `output-${i}.csv`);
            const partData = fs.readFileSync(partPath, 'utf8');
            await uploadCSVToSalesforce(partData, conn, salesforceObjectAPIName); // Only upload logic
            fs.unlinkSync(partPath); // Delete the part file after uploading
        }
        fs.unlinkSync(tempCsvPath); // Delete the original large CSV file
        fs.rmdirSync(tempDir); // Remove the temporary directory
        res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' }); // Send response here
    } catch (error) {
        console.error('Error during processing:', error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Failed to process files', error: error.toString() });
        }
    }
}

async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
    // Perform upload tasks without handling the response
    const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
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

    await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
        state: 'UploadComplete'
    }, {
        headers: {
            Authorization: `Bearer ${conn.accessToken}`,
            'Content-Type': 'application/json'
        }
    });
}

*/







/*

const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parse');
const { stringify } = require('csv-stringify/sync');

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
        version: '54.0',
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

        await splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res);
    } catch (error) {
        console.error('Failed to process files:', error);
        res.status(500).json({ message: 'Failed to process files', error: error.response ? error.response.data : error.toString() });
    }
});

const PORT = process.env.PORT || 3333;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

async function splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res) {
    const maxFileSize = 150 * 1024 * 1024; // Salesforce max size limit (150 MB)
    let currentSize = 0;
    let fileNumber = 0;
    let currentRecords = [];
    let header = [];

    const parser = csv.parse(csvData, { columns: true, skip_empty_lines: true, relax_column_count: true });

    parser.on('readable', function () {
        let record;
        while (record = parser.read()) {
            if (header.length === 0) header = Object.keys(record);
            const recordString = stringify([record], { header: false });
            const recordSize = Buffer.byteLength(recordString);

            if (currentSize + recordSize > maxFileSize) {
                writeRecordsToFile(`output-${fileNumber}.csv`, header, currentRecords);
                fileNumber++;
                currentRecords = [];
                currentSize = 0;
            }
            currentRecords.push(record);
            currentSize += recordSize;
        }
    });

    parser.on('end', function () {
        if (currentRecords.length > 0) {
            writeRecordsToFile(`output-${fileNumber}.csv`, header, currentRecords);
        }
        res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' });
    });

    parser.on('error', function (error) {
        console.error('Error while parsing CSV:', error);
        res.status(500).json({ message: 'Failed to parse CSV', error: error.toString() });
    });
}

function writeRecordsToFile(fileName, header, records) {
    const output = fs.createWriteStream(fileName);
    const recordsString = stringify(records, { header: true, columns: header });
    output.write(recordsString);
    output.close();
}

*/




/* 
const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const fs = require('fs');
const csvSplitStream = require('csv-split-stream');

const path = require('path');

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
        version: '54.0',
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
            // Split and upload if CSV data is larger than 150 MB
            await splitAndUploadCSV(csvData, conn, salesforceObjectAPIName, res);
        } else {
            // Upload directly if under the limit
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
                lineLimit: 10000
            },
            (index) => fs.createWriteStream(path.join(tempDir, `output-${index}.csv`))
        );

        console.log('CSV split into ' + result.totalChunks + ' files.');


        for (let i = 0; i < result.totalChunks; i++) {
            const partPath = path.join(tempDir, `output-${i}.csv`);
            const partData = fs.readFileSync(partPath, 'utf8');
            await uploadCSVToSalesforce(partData, conn, salesforceObjectAPIName); // Only upload logic
            fs.unlinkSync(partPath); // Delete the part file after uploading
        }
        fs.unlinkSync(tempCsvPath); // Delete the original large CSV file
        fs.rmdirSync(tempDir); // Remove the temporary directory
        res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.' }); // Send response here
    } catch (error) {
        console.error('Error during processing:', error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Failed to process files', error: error.toString() });
        }
    }
}

async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
    // Perform upload tasks without handling the response
    const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
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

    await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
        state: 'UploadComplete'
    }, {
        headers: {
            Authorization: `Bearer ${conn.accessToken}`,
            'Content-Type': 'application/json'
        }
    });
}

*/












// const express = require('express');
// const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
// const jsforce = require('jsforce');
// const axios = require('axios');  // Make sure to require axios for HTTP requests
// const { parse } = require('csv-parse/sync');

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

// const streamToString = (stream) =>
//     new Promise((resolve, reject) => {
//         const chunks = [];
//         stream.on('data', (chunk) => chunks.push(chunk));
//         stream.on('error', reject);
//         stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
//     });

// app.post('/loadData', async (req, res) => {
//     const { fileName, bucketName, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;

//     const conn = new jsforce.Connection({
//         version: '54.0',
//         loginUrl: salesforceLoginUrl
//     });

//     try {
//         await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
//         console.log('Successfully logged into Salesforce with API version:', conn.version);

//         const getObjectParams = {
//             Bucket: bucketName,
//             Key: fileName
//         };
//         const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
//         const csvData = await streamToString(Body);

//         const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
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

//         console.log('Job response:', jobCreationResponse.data);

//         const uploadURL = `${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`;
//         const uploadResponse = await axios.put(uploadURL, csvData, {
//             headers: {
//                 'Content-Type': 'text/csv',
//                 Authorization: `Bearer ${conn.accessToken}`
//             }
//         });

//         if (uploadResponse.status === 201) {
//             await axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//                 state: 'UploadComplete'
//             }, {
//                 headers: {
//                     Authorization: `Bearer ${conn.accessToken}`,
//                     'Content-Type': 'application/json'
//                 }
//             });

//             console.log(`Job ${jobCreationResponse.data.id} closed and batch uploaded.`);
//             res.status(200).json({ message: 'Data successfully processed and uploaded to Salesforce.' });
//         } else {
//             throw new Error('Failed to upload data to Salesforce');
//         }
//     } catch (error) {
//         console.error('Failed to process files:', error);
//         res.status(500).json({ message: 'Failed to process files', error: error.response ? error.response.data : error.toString() });
//     }
// });

// const PORT = process.env.PORT || 3333;
// app.listen(PORT, () => console.log(`Server running on port ${PORT}`));





































// const express = require('express');
// const { S3Client, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
// const jsforce = require('jsforce');
// const sfbulk = require('node-sf-bulk2');
// const { parse } = require('csv-parse/sync');
// const bodyParser = require('body-parser');

// const app = express();
// app.use(bodyParser.json());

// // Configure AWS and Salesforce credentials
// const awsCredentials = require('../resource/awsConfig.json');

// const s3Client = new S3Client(awsCredentials);

// const conn = new jsforce.Connection({
//   loginUrl: 'https://dataxform.my.salesforce.com'
// });

// async function loginToSalesforce(username, password, token) {
//   try {
//     await conn.login(username, password + token);
//     console.log('Successfully logged into Salesforce.');
//     return conn;
//   } catch (error) {
//     console.error('Salesforce login failed:', error);
//     throw error;
//   }
// }

// async function processBulkData(data, objectName, conn) {
//   const bulkApi = new sfbulk.BulkAPI2(conn);
//   const job = await bulkApi.createJob(objectName, 'insert');

//   try {
//     const batch = await bulkApi.createAndUploadBatch(job.id, data);
//     console.log(`Batch ${batch.id} queued.`);
//     await bulkApi.closeJob(job.id);
//     return batch;
//   } catch (error) {
//     console.error('Error processing bulk job:', error);
//     await bulkApi.abortJob(job.id);
//     throw error;
//   }
// }

// app.post('/loadData', async (req, res) => {
//   const { bucketName, folderName, salesforceUsername, salesforcePassword, salesforceToken, salesforceObjectAPIName } = req.query;

//   const conn = await loginToSalesforce(salesforceUsername, salesforcePassword, salesforceToken);

//   try {
//     const listParams = {
//       Bucket: bucketName,
//       Prefix: folderName
//     };
//     const { Contents } = await s3Client.send(new ListObjectsV2Command(listParams));

//     if (!Contents || Contents.length === 0) {
//       return res.status(404).json({ message: 'No files found in the specified folder' });
//     }

//     let allData = "";

//     for (const object of Contents) {
//       if (!object.Key.endsWith('.csv')) continue;
      
//       const { Body } = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: object.Key }));
//       const data = await streamToString(Body);
//       allData += data;  // Assuming CSV data needs to be combined
//     }

//     const batchInfo = await processBulkData(allData, salesforceObjectAPIName, conn);
//     res.status(200).json({ message: 'Bulk data processing initiated', details: batchInfo });
//   } catch (error) {
//     console.error('Failed to process files:', error);
//     res.status(500).json({ message: 'Failed to process files', error: error.toString() });
//   }
// });

// const PORT = process.env.PORT || 3333;
// app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// async function streamToString(stream) {
//   const chunks = [];
//   return new Promise((resolve, reject) => {
//     stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
//     stream.on('error', reject);
//     stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
//        });
// }

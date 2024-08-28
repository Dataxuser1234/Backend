// const express = require('express');
// const { S3Client, GetObjectCommand, ListObjectsCommand } = require('@aws-sdk/client-s3');
// const jsforce = require('jsforce');
// const axios = require('axios');
// const fs = require('fs');
// const path = require('path');
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

// function setupLogger(requestId) {
//     const logsDir = path.join(__dirname, 'logs');
//     if (!fs.existsSync(logsDir)) {
//         fs.mkdirSync(logsDir);
//     }
//     const logFilename = `log-${requestId}.txt`;
//     const filePath = path.join(logsDir, logFilename);
//     return filePath;
// }

// function writeLog(filePath, message, data) {
//     const timestamp = new Date().toISOString();
//     const logEntry = `${timestamp} - ${message} - ${JSON.stringify(data, null, 2)}\n`;
//     fs.appendFileSync(filePath, logEntry);
// }

// async function retryOperation(operation, delay, retries, logPath) {
//     try {
//         return await operation();
//     } catch (error) {
//         if (retries > 0) {
//             writeLog(logPath, 'Retrying after error', { error: error.message, attemptsLeft: retries });
//             await new Promise(resolve => setTimeout(resolve, delay));
//             return retryOperation(operation, delay, retries - 1, logPath);
//         }
//         throw error;
//     }
// }

// async function fetchJobFailureDetails(conn, jobId, logPath) {
//     const resultsUrl = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobId}/failedResults`;
//     try {
//         const response = await axios.get(resultsUrl, {
//             headers: {
//                 Authorization: `Bearer ${conn.accessToken}`
//             }
//         });
//         writeLog(logPath, 'Response ',response );
//         const failedRecords = response.data.records || [];
//         writeLog(logPath, 'Failed Record Details', failedRecords);
//         return failedRecords;
//     } catch (error) {
//         writeLog(logPath, 'Error fetching job failure details', { jobId, error: error.message });
//         return [];
//     }
// }

// async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName, logPath) {
//     const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
//     const jobCreationResponse = await axios.post(apiURL, {
//         object: salesforceObjectAPIName,
//         operation: 'insert',
//         contentType: 'CSV'
//     }, {
//         headers: {
//             Authorization: `Bearer ${conn.accessToken}`,
//             'Content-Type': 'application/json'
//         }
//     });

//     await axios.put(`${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`, csvData, {
//         headers: {
//             'Content-Type': 'text/csv',
//             Authorization: `Bearer ${conn.accessToken}`
//         }
//     });

//     await retryOperation(() => axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//         state: 'UploadComplete'
//     }, {
//         headers: {
//             Authorization: `Bearer ${conn.accessToken}`,
//             'Content-Type': 'application/json'
//         }
//     }), 5000, 3, logPath);

//     let jobDetailsResponse, jobComplete = false;
//     do {
//         await new Promise(resolve => setTimeout(resolve, 10000));
//         jobDetailsResponse = await axios.get(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//             headers: {
//                 Authorization: `Bearer ${conn.accessToken}`
//             }
//         });
//         jobComplete = jobDetailsResponse.data.state === 'JobComplete';
//         if (jobDetailsResponse.data.numberRecordsFailed > 0) {
//             const failureDetails = await fetchJobFailureDetails(conn, jobCreationResponse.data.id, logPath);
//             writeLog(logPath, 'Records failed during upload', { JobId: jobCreationResponse.data.id, FailureDetails: failureDetails });
//         }
//     } while (!jobComplete);

//     return {
//         JobId: jobCreationResponse.data.id,
//         StartTime: jobDetailsResponse.data.createdDate,
//         EndTime: jobDetailsResponse.data.systemModstamp,
//         Status: jobDetailsResponse.data.state,
//         Operation: jobCreationResponse.data.operation,
//         contentType: jobCreationResponse.data.contentType,
//         jobType: "Bulk V2",
//         Object: jobCreationResponse.data.object,
//         RecordsProcessed: jobDetailsResponse.data.numberRecordsProcessed,
//         RecordsFailed: jobDetailsResponse.data.numberRecordsFailed,
//         FailedRecordsDetails: jobDetailsResponse.data.numberRecordsFailed > 0 ? await fetchJobFailureDetails(conn, jobCreationResponse.data.id, logPath) : [],
//         TimeToComplete: `${Math.floor((new Date(jobDetailsResponse.data.systemModstamp) - new Date(jobDetailsResponse.data.createdDate)) / 60000)} minutes`
//     };
// }

// app.post('/loadData', async (req, res) => {
//     const requestId = new Date().toISOString().replace(/[:\.]/g, '-');
//     const logPath = setupLogger(requestId);
//     const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;
//     const conn = new jsforce.Connection({ loginUrl: salesforceLoginUrl });

//     try {
//         await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
//         writeLog(logPath, 'Logged into Salesforce', { apiVersion: conn.version });

//         const listParams = { Bucket: bucketName, Prefix: folderPath };
//         const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));
//         const jobPromises = Contents.filter(file => file.Size > 0 && !file.Key.endsWith('/')).map(async file => {
//             const getObjectParams = { Bucket: bucketName, Key: file.Key };
//             const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
//             const chunks = [];
//             for await (const chunk of Body) { chunks.push(chunk); }
//             const csvData = Buffer.concat(chunks).toString('utf8');
//             return uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName, logPath);
//         });

//         const jobDetails = (await Promise.all(jobPromises)).filter(job => job !== null);
//         const totalRecordsProcessed = jobDetails.reduce((acc, job) => acc + job.RecordsProcessed, 0);
//         const totalRecordsFailed = jobDetails.reduce((acc, job) => acc + job.RecordsFailed, 0);
//         res.status(200).json({
//             message: 'All data successfully processed and uploaded to Salesforce.',
//             totalRecordsProcessed,
//             FailedRecords: totalRecordsFailed,
//             jobDetails
//         });
//     } catch (error) {
//         writeLog(logPath, 'Failed to process files', { error: error.message });
//         res.status(500).json({ message: 'Failed to process files', error: error.message });
//     }
// });

// const PORT = process.env.PORT || 3333;
// app.listen(PORT, () => {
//     const requestId = new Date().toISOString().replace(/[:\.]/g, '-');
//     const logPath = setupLogger(requestId);
//     writeLog(logPath, 'Server running', { port: PORT });
// });





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

function log(level, message, data = {}) {
    console.log(JSON.stringify({ level, message, timestamp: new Date().toISOString(), ...data }));
}

async function retryOperation(operation, delay, retries) {
    try {
        return await operation();
    } catch (error) {
        if (retries > 0) {
            log('warning', `Retrying after error: ${error.message}, attempts left: ${retries}`, { detail: error });
            await new Promise(resolve => setTimeout(resolve, delay));
            return retryOperation(operation, delay, retries - 1);
        }
        throw error;
    }
}

async function fetchJobFailureDetails(conn, jobId) {
    const resultsUrl = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobId}/failedResults`;
    
    try {
        const response = await axios.get(resultsUrl, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`
            }
        });
        console.log('Response ',response)
        console.log('Failed Records Details:', response.data);
        return response.data.records || [];
    } catch (error) {
        log('error', 'Failed to fetch job failure details', { jobId, error: error.message });
        console.error('Error fetching failure details:', error.response ? error.response.data : error);
        return [];
    }
}

// Failed Records Details: 
// Failure details for job 750Hp00001KsGPgIAN: []
// {"level":"warning","message":"Some records failed during upload","timestamp":"2024-08-20T18:21:25.411Z","JobId":"750Hp00001KsGPgIAN","FailureDetails":[]}

async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
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

    await axios.put(`${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`, csvData, {
        headers: {
            'Content-Type': 'text/csv',
            Authorization: `Bearer ${conn.accessToken}`
        }
    });

    await retryOperation(() => axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
        state: 'UploadComplete'
    }, {
        headers: {
            Authorization: `Bearer ${conn.accessToken}`,
            'Content-Type': 'application/json'
        }
    }), 5000, 3);

    let jobDetailsResponse, jobComplete = false;
    do {
        await new Promise(resolve => setTimeout(resolve, 10000));
        jobDetailsResponse = await axios.get(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`
            }
        });
        jobComplete = jobDetailsResponse.data.state === 'JobComplete';
        if (jobDetailsResponse.data.numberRecordsFailed > 0) {
            const failureDetails = await fetchJobFailureDetails(conn, jobCreationResponse.data.id);
            console.log(`Failure details for job ${jobCreationResponse.data.id}:`, failureDetails);
            log('warning', 'Some records failed during upload', { JobId: jobCreationResponse.data.id, FailureDetails: failureDetails });
        }
    } while (!jobComplete);

    return {
        JobId: jobCreationResponse.data.id,
        StartTime: jobDetailsResponse.data.createdDate,
        EndTime: jobDetailsResponse.data.systemModstamp,
        Status: jobDetailsResponse.data.state,
        Operation: jobCreationResponse.data.operation,
        contentType: jobCreationResponse.data.contentType,
        jobType: "Bulk V2",
        Object: jobCreationResponse.data.object,
        RecordsProcessed: jobDetailsResponse.data.numberRecordsProcessed,
        RecordsFailed: jobDetailsResponse.data.numberRecordsFailed,
        FailedRecordsDetails: jobDetailsResponse.data.numberRecordsFailed > 0 ? await fetchJobFailureDetails(conn, jobCreationResponse.data.id) : [],
        TimeToComplete: `${Math.floor((new Date(jobDetailsResponse.data.systemModstamp) - new Date(jobDetailsResponse.data.createdDate)) / 60000)} minutes`
    };
}

app.post('/loadData', async (req, res) => {
    const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;
    const conn = new jsforce.Connection({ loginUrl: salesforceLoginUrl });

    try {
        await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
        log('info', 'Logged into Salesforce', { apiVersion: conn.version });

        const listParams = { Bucket: bucketName, Prefix: folderPath };
        const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));
        const jobPromises = Contents.filter(file => file.Size > 0 && !file.Key.endsWith('/')).map(async file => {
            const getObjectParams = { Bucket: bucketName, Key: file.Key };
            const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
            const chunks = [];
            for await (const chunk of Body) { chunks.push(chunk); }
            const csvData = Buffer.concat(chunks).toString('utf8');
            return uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName);
        });

        const jobDetails = (await Promise.all(jobPromises)).filter(job => job !== null);
        const totalRecordsProcessed = jobDetails.reduce((acc, job) => acc + job.RecordsProcessed, 0);
        const totalRecordsFailed = jobDetails.reduce((acc, job) => acc + job.RecordsFailed, 0);
        res.status(200).json({
            message: 'All data successfully processed and uploaded to Salesforce.',
            totalRecordsProcessed,
            FailedRecords: totalRecordsFailed,
            jobDetails
        });
    } catch (error) {
        log('error', 'Failed to process files', { error: error.message });
        res.status(500).json({ message: 'Failed to process files', error: error.message });
    }
});

const PORT = process.env.PORT || 3333;
app.listen(PORT, () => log('info', `Server running on port ${PORT}`));





/*


working code




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

// Function to log messages
function log(level, message, data = {}) {
    console.log(JSON.stringify({ level, message, timestamp: new Date().toISOString(), ...data }));
}

// Function to handle retries
async function retryOperation(operation, delay, retries) {
    try {
        return await operation();
    } catch (error) {
        if (retries > 0) {
            log('warning', `Retrying after error: ${error.message}, attempts left: ${retries}`, { detail: error });
            await new Promise(resolve => setTimeout(resolve, delay));
            return retryOperation(operation, delay, retries - 1);
        }
        throw error;
    }
}

// Function to upload CSV to Salesforce
async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName) {
    try {
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

        await axios.put(`${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`, csvData, {
            headers: {
                'Content-Type': 'text/csv',
                Authorization: `Bearer ${conn.accessToken}`
            }
        });

        await retryOperation(() => axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
            state: 'UploadComplete'
        }, {
            headers: {
                Authorization: `Bearer ${conn.accessToken}`,
                'Content-Type': 'application/json'
            }
        }), 5000, 3);

        let jobDetailsResponse, jobComplete = false;
        do {
            await new Promise(resolve => setTimeout(resolve, 10000));
            jobDetailsResponse = await axios.get(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
                headers: {
                    Authorization: `Bearer ${conn.accessToken}`
                }
            });
            jobComplete = jobDetailsResponse.data.state === 'JobComplete';
        } while (!jobComplete);

        return {
            JobId: jobCreationResponse.data.id,
            StartTime: jobDetailsResponse.data.createdDate,
            EndTime: jobDetailsResponse.data.systemModstamp,
            Status: jobDetailsResponse.data.state,
            Operation: jobCreationResponse.data.operation,
            contentType: jobCreationResponse.data.contentType,
            jobType: "Bulk V2",
            Object: jobCreationResponse.data.object,
            RecordsProcessed: jobDetailsResponse.data.numberRecordsProcessed,
            RecordsFailed: jobDetailsResponse.data.numberRecordsFailed,
            TimeToComplete: `${Math.floor((new Date(jobDetailsResponse.data.systemModstamp) - new Date(jobDetailsResponse.data.createdDate)) / 60000)} minutes`
        };
    } catch (error) {
        log('error', 'Error uploading to Salesforce', { error: JSON.stringify(error.response ? error.response.data : error) });
        throw error;
    }
}

app.post('/loadData', async (req, res) => {
    const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;
    const conn = new jsforce.Connection({ loginUrl: salesforceLoginUrl });

    try {
        await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
        log('info', 'Logged into Salesforce', { apiVersion: conn.version });

        const listParams = { Bucket: bucketName, Prefix: folderPath };
        console.log('List :',listParams)
        const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));
        console.log('Contents :', Contents)
        const jobPromises = Contents.filter(file => file.Size > 0 && !file.Key.endsWith('/')).map(async file => {
            const getObjectParams = { Bucket: bucketName, Key: file.Key };
            const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
            const chunks = [];
            for await (const chunk of Body) { chunks.push(chunk); }
            const csvData = Buffer.concat(chunks);
            if (csvData.length > 150000000) {
                log('warning', 'Data exceeds Salesforce size limit', { fileKey: file.Key, size: csvData.length });
                return null;
            } else {
                return uploadCSVToSalesforce(csvData.toString('utf8'), conn, salesforceObjectAPIName);
            }
        });

        const jobDetails = (await Promise.all(jobPromises)).filter(job => job !== null);
        const totalRecordsProcessed = jobDetails.reduce((acc, job) => acc + job.RecordsProcessed, 0);
        const totalRecordsFailed = jobDetails.reduce((acc, job) => acc + job.RecordsFailed, 0);
        log('info', 'All jobs processed', { jobCount: jobDetails.length });
        res.status(200).json({
            message: 'All data successfully processed and uploaded to Salesforce.',
            totalRecordsProcessed: totalRecordsProcessed,
            FailedRecords: totalRecordsFailed,
            jobDetails
        });
    } catch (error) {
        log('error', 'Failed to process files', { error: error.message, stack: error.stack });
        res.status(500).json({ message: 'Failed to process files', error: error.message });
    }
});

const PORT = process.env.PORT || 3333;
app.listen(PORT, () => log('info', `Server running on port ${PORT}`));



*/



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

// // Retry Logic
// async function retryOperation(operation, delay, retries) {
//     try {
//         return await operation();
//     } catch (error) {
//         if (retries > 0) {
//             console.log(`Retrying after error: ${error.message}, attempts left: ${retries}`);
//             await new Promise(resolve => setTimeout(resolve, delay));
//             return retryOperation(operation, delay, retries - 1);
//         }
//         throw error;
//     }
// }

// // Log utility
// function log(level, message, data = {}) {
//     console.log(JSON.stringify({ level, message, timestamp: new Date().toISOString(), ...data }));
// }

// // Error handling middleware
// app.use((err, req, res, next) => {
//     log('error', 'An error occurred', { error: err.message });
//     res.status(500).json({ error: 'Internal server error', details: err.message });
// });

// // Define the uploadCSVToSalesforce function here
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

//         await axios.put(`${conn.instanceUrl}/${jobCreationResponse.data.contentUrl}`, csvData, {
//             headers: {
//                 'Content-Type': 'text/csv',
//                 Authorization: `Bearer ${conn.accessToken}`
//             }
//         });

//         await retryOperation(() => axios.patch(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//             state: 'UploadComplete'
//         }, {
//             headers: {
//                 Authorization: `Bearer ${conn.accessToken}`,
//                 'Content-Type': 'application/json'
//             }
//         }), 5000, 3);

//         let jobDetailsResponse;
//         let jobComplete = false;
//         do {
//             await new Promise(resolve => setTimeout(resolve, 10000));
//             jobDetailsResponse = await axios.get(`${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest/${jobCreationResponse.data.id}`, {
//                 headers: {
//                     Authorization: `Bearer ${conn.accessToken}`
//                 }
//             });
//             jobComplete = jobDetailsResponse.data.state === 'JobComplete';
//         } while (!jobComplete);

//         return {
//             JobId: jobCreationResponse.data.id,
//             StartTime: jobDetailsResponse.data.createdDate,
//             EndTime: jobDetailsResponse.data.systemModstamp,
//             Status: jobDetailsResponse.data.state,
//             Operation: jobCreationResponse.data.operation,
//             contentType: jobCreationResponse.data.contentType,
//             jobType: "Bulk V2",
//             Object: jobCreationResponse.data.object,
//             RecordsProcessed: jobDetailsResponse.data.numberRecordsProcessed,
//             RecordsFailed: jobDetailsResponse.data.numberRecordsFailed,
//             TimeToComplete: `${Math.floor((new Date(jobDetailsResponse.data.systemModstamp) - new Date(jobDetailsResponse.data.createdDate)) / 60000)} minutes`
//         };
//     } catch (error) {
//         log('error', 'Failed to upload CSV to Salesforce', { error: JSON.stringify(error.response ? error.response.data : error) });
//         throw new Error(`Failed to upload CSV to Salesforce: ${JSON.stringify(error.response ? error.response.data : error)}`);
//     }
// }

// app.post('/loadData', async (req, res) => {
//     const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName } = req.query;
//     const conn = new jsforce.Connection({ loginUrl: salesforceLoginUrl });
//     try {
//         await conn.login(salesforceUsername, salesforcePassword + salesforceToken);
//         log('info', 'Successfully logged into Salesforce', { apiVersion: conn.version });

//         const listParams = { Bucket: bucketName, Prefix: folderPath };
//         const { Contents } = await s3Client.send(new ListObjectsCommand(listParams));
//         const jobPromises = Contents.filter(file => file.Size > 0 && !file.Key.endsWith('/')).map(async file => {
//             const getObjectParams = { Bucket: bucketName, Key: file.Key };
//             const { Body } = await s3Client.send(new GetObjectCommand(getObjectParams));
//             const chunks = [];
//             for await (const chunk of Body) { chunks.push(chunk); }
//             const csvData = Buffer.concat(chunks);
//             if (csvData.length > 150000000) {
//                 log('warning', 'Data exceeds the Salesforce size limit', { fileKey: file.Key, size: csvData.length });
//                 return null;
//             } else {
//                 return uploadCSVToSalesforce(csvData.toString('utf8'), conn, salesforceObjectAPIName);
//             }
//         });

//         const jobDetails = (await Promise.all(jobPromises)).filter(job => job !== null);
//         log('info', 'All jobs processed', { jobCount: jobDetails.length });
//         res.status(200).json({ message: 'All data successfully processed and uploaded to Salesforce.', jobDetails });
//     } catch (error) {
//         log('error', 'Failed to process files', { error: error.message, stack: error.stack });
//         res.status(500).json({ message: 'Failed to process files', error: error.message });
//     }
// });

// const PORT = 3333;
// app.listen(PORT, () => log('info', `Server running on port ${PORT}`));


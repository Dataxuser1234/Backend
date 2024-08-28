const AWS = require('aws-sdk');
const express = require('express');
const { S3Client, GetObjectCommand, ListObjectsCommand } = require('@aws-sdk/client-s3');
const jsforce = require('jsforce');
const axios = require('axios');
const app = express();
app.use(express.json());

const awsCredentials = require('../resource/awsConfig.json');


AWS.config.update({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});

const cwLogs = new AWS.CloudWatchLogs();
const logGroupName = '/load-into-salesforce';
const logStreamName = `logStream-${new Date().toISOString().split('T')[0]}`; 
let sequenceToken = null; 

async function ensureLogInfrastructure() {
    try {
        await cwLogs.createLogGroup({ logGroupName }).promise();
    } catch (error) {
        if (error.code !== 'ResourceAlreadyExistsException') {
            console.error('Failed to create log group:', error);
            return;
        }
    }

    try {
        await cwLogs.createLogStream({ logGroupName, logStreamName }).promise();
    } catch (error) {
        if (error.code !== 'ResourceAlreadyExistsException') {
            console.error('Failed to create log stream:', error);
            return;
        }
    }
}

async function logToCloudWatch(level, message, data = {}) {
    const params = {
        logGroupName,
        logStreamName,
        logEvents: [{
            message: JSON.stringify({ level, message, timestamp: new Date().toISOString(), ...data }),
            timestamp: Date.now()
        }],
        sequenceToken
    };

    try {
        const response = await cwLogs.putLogEvents(params).promise();
        sequenceToken = response.nextSequenceToken;
    } catch (error) {
        if (error.code === 'InvalidSequenceTokenException') {
            sequenceToken = error.message.match(/sequenceToken is: (.*?) /)[1];
            await logToCloudWatch(level, message, data); 
        } else {
            console.error('Failed to log event to CloudWatch:', error);
        }
    }
}

const s3Client = new S3Client({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});


async function retryOperation(operation, delay, retries, factor = 2) {
    try {
        return await operation();
    } catch (error) {
        if (retries > 0) {
            const newDelay = delay * factor;
            logToCloudWatch('warning', `Retrying after error: ${error.message}`, {
                nextAttemptInMs: newDelay, attemptsLeft: retries, detail: error
            });
            await new Promise(resolve => setTimeout(resolve, newDelay));
            return retryOperation(operation, newDelay, retries - 1, factor);
        }
        logToCloudWatch('error', 'Operation failed after all retries', { error: error.message });
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
        logToCloudWatch('info', 'Failed Records Details:', { jobId, response: response.data });
        return response.data.records || [];
    } catch (error) {
        logToCloudWatch('error', 'Failed to fetch job failure details', { jobId, error: error.message });
        throw error;
    }
}
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
    }), 5000, 3, 2);

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
            logToCloudWatch('warning', 'Some records failed during upload', { JobId: jobCreationResponse.data.id, FailureDetails: failureDetails });
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
        logToCloudWatch('info', 'Logged into Salesforce', { apiVersion: conn.version });

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
        logToCloudWatch('error', 'Failed to process files', { error: error.message });
        res.status(500).json({ message: 'Failed to process files', error: error.message });
    }
});

const PORT = process.env.PORT || 3334;
app.listen(PORT, async () => {
    await ensureLogInfrastructure();
    logToCloudWatch('info', `Server running on port ${PORT}`);
});


/*

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

async function retryOperation(operation, delay, retries, factor = 2) {
    try {
        return await operation();
    } catch (error) {
        if (retries > 0) {
            if (error.message.includes('UNABLE_TO_LOCK_ROW')) {
                const newDelay = delay * factor;
                log('warning', `Retrying after lock error: ${error.message}, next attempt in ${newDelay}ms, attempts left: ${retries}`, { detail: error });
                await new Promise(resolve => setTimeout(resolve, newDelay));
                return retryOperation(operation, newDelay, retries - 1, factor);
            }
            log('warning', `Retrying after error: ${error.message}, next attempt in ${delay}ms, attempts left: ${retries}`, { detail: error });
            await new Promise(resolve => setTimeout(resolve, delay));
            return retryOperation(operation, delay, retries - 1, factor);
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
        log('info', 'Failed Records Details:', { jobId, response: response.data });
        return response.data.records || [];
    } catch (error) {
        log('error', 'Failed to fetch job failure details', { jobId, error: error.message });
        throw error;
    }
}

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
    }), 5000, 3, 2);

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

const PORT = process.env.PORT || 3334;
app.listen(PORT, () => log('info', `Server running on port ${PORT}`));


*/
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
        log('info', 'Failed Records Details:', { response: response.data });
        return response.data.records || [];
    } catch (error) {
        log('error', 'Failed to fetch job failure details', { jobId, error: error.message });
        return [];
    }
}

async function uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName, operationType, externalIdFieldName = null) {
    const apiURL = `${conn.instanceUrl}/services/data/v${conn.version}/jobs/ingest`;
    const jobCreationResponse = await axios.post(apiURL, {
        object: salesforceObjectAPIName,
        operation: operationType,
        contentType: 'CSV',
        lineEnding: 'LF',
        ...(operationType === 'upsert' ? { externalIdFieldName } : {})
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
            log('warning', 'Some records failed during upload', { JobId: jobCreationResponse.data.id, FailureDetails: failureDetails });
        }
    } while (!jobComplete);

    return {
        JobId: jobCreationResponse.data.id,
        StartTime: jobDetailsResponse.data.createdDate,
        EndTime: jobDetailsResponse.data.systemModstamp,
        Status: jobDetailsResponse.data.state,
        Operation: jobDetailsResponse.data.operation,
        contentType: jobDetailsResponse.data.contentType,
        jobType: "Bulk V2",
        Object: jobCreationResponse.data.object,
        RecordsProcessed: jobDetailsResponse.data.numberRecordsProcessed,
        RecordsFailed: jobDetailsResponse.data.numberRecordsFailed,
        FailedRecordsDetails: jobDetailsResponse.data.numberRecordsFailed > 0 ? await fetchJobFailureDetails(conn, jobCreationResponse.data.id) : [],
        TimeToComplete: `${Math.floor((new Date(jobDetailsResponse.data.systemModstamp) - new Date(jobDetailsResponse.data.createdDate)) / 60000)} minutes`
    };
}

app.post('/loadData', async (req, res) => {
    const { bucketName, folderPath, salesforceUsername, salesforcePassword, salesforceToken, salesforceLoginUrl, salesforceObjectAPIName, operationType, externalIdFieldName } = req.query;
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
            let csvData = Buffer.concat(chunks).toString('utf8');
            //csvData = csvData.replace(/\n/g, '\r\n');
            csvData = csvData.replace(/\r\n/g, '\n');
            return uploadCSVToSalesforce(csvData, conn, salesforceObjectAPIName, operationType, externalIdFieldName);
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

const PORT = process.env.PORT || 3399;
app.listen(PORT, () => log('info', `Server running on port ${PORT}`));

const express = require('express');
const aws = require('aws-sdk');
const bodyParser = require('body-parser');
const app = express();
const port = 3001;

// Load AWS credentials from a configuration file
const awsCredentials = require('../resource/awsConfig.json');

const s3 = new aws.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});

app.use(bodyParser.json());

app.post('/merge-files', async (req, res) => {
    const { sourceBucket, sourcePrefix, targetBucket, targetPrefix, outputFile } = req.query;
    console.log(`Received request to merge files from ${sourcePrefix} in ${sourceBucket} to ${targetPrefix}/${outputFile} in ${targetBucket}`);

    try {
        // Check and create the target prefix if it does not exist
        await ensureFolderExists(targetBucket, targetPrefix);
        console.log(`Target prefix ${targetPrefix} verified or created in ${targetBucket}`);

        // List all files in the source prefix
        const listParams = {
            Bucket: sourceBucket,
            Prefix: sourcePrefix
        };
        const listedObjects = await s3.listObjectsV2(listParams).promise();
        const files = listedObjects.Contents.filter(item => !item.Key.endsWith('/'));

        let mergedData = '';
        let currentFileSize = 0;
        const maxFileSize = 150 * 1024 * 1024; // 150 MB 
        let currentFileNumber = 1;
        let header = '';
        
        
        for (let file of files) {
            const readParams = {
                Bucket: sourceBucket,
                Key: file.Key
            };
            const data = await s3.getObject(readParams).promise();
            const records = data.Body.toString('utf-8').split('\n');

            if (header === '') {
                header = records[0]; 
            }

            for (let i = 1; i < records.length; i++) { // Start from 1 to skip the header
                let record = records[i] + '\n';
                if (currentFileSize + Buffer.byteLength(record, 'utf-8') > maxFileSize) {
                    
                    await writeToFile(targetBucket, targetPrefix, outputFile, mergedData, currentFileNumber);
                    currentFileNumber++;
                    mergedData = header + '\n' + record; // Start a new file with the header
                    currentFileSize = Buffer.byteLength(mergedData, 'utf-8');
                } else {
                    
                    mergedData += record;
                    currentFileSize += Buffer.byteLength(record, 'utf-8');
                }
            }
        }

        if (mergedData.length > 0) {
            await writeToFile(targetBucket, targetPrefix, outputFile, mergedData, currentFileNumber);
        }

        console.log(`Files merged into ${currentFileNumber} parts`);
        res.send({ message: `Files merged successfully into ${currentFileNumber} parts` });
    } catch (error) {
        console.error('Failed to merge files:', error);
        res.status(500).send({ error: 'Failed to merge files', details: error.message });
    }
});

async function writeToFile(bucket, prefix, baseFileName, data, fileNumber) {
    const fileName = `${prefix}/${baseFileName}_${fileNumber}.csv`;
    console.log(`Writing merged data to ${fileName} in ${bucket}`);
    const writeParams = {
        Bucket: bucket,
        Key: fileName,
        Body: data
    };
    await s3.putObject(writeParams).promise();
}

async function ensureFolderExists(bucket, prefix) {
    if (!prefix.endsWith('/')) {
        prefix += '/';
    }
    try {
        await s3.headObject({
            Bucket: bucket,
            Key: prefix
        }).promise();
    } catch (error) {
        if (error.code === 'NotFound') {
            await s3.putObject({
                Bucket: bucket,
                Key: prefix
            }).promise();
        } else {
            throw error;
        }
    }
}

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});





// const express = require('express');
// const aws = require('aws-sdk');
// const bodyParser = require('body-parser');
// const app = express();
// const port = 3001;

// // Load AWS credentials from a configuration file
// const awsCredentials = require('../resource/awsConfig.json');

// const s3 = new aws.S3({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// app.use(bodyParser.json());

// app.post('/merge-files', async (req, res) => {
//     const { sourceBucket, sourcePrefix, targetBucket, targetPrefix, outputFile } = req.query;
//     console.log(`Received request to merge files from ${sourcePrefix} in ${sourceBucket} to ${targetPrefix}/${outputFile} in ${targetBucket}`);

//     try {
//         // Check and create the target prefix if it does not exist
//         await ensureFolderExists(targetBucket, targetPrefix);
//         console.log(`Target prefix ${targetPrefix} verified or created in ${targetBucket}`);

//         // List all files in the source prefix
//         const listParams = {
//             Bucket: sourceBucket,
//             Prefix: sourcePrefix
//         };
//         console.log(`Listing files with prefix ${sourcePrefix} in bucket ${sourceBucket}`);
//         const listedObjects = await s3.listObjectsV2(listParams).promise();

//         const files = listedObjects.Contents.filter(item => !item.Key.endsWith('/'));
//         console.log(`Found ${files.length} files to merge`);

//         let mergedData = '';
//         let totalRecords = 0;
//         let isFirstFile = true; // Flag to check if it's the first file

//         // Read each file and concatenate its contents
//         for (let file of files) {
//             console.log(`Reading file: ${file.Key}`);
//             const readParams = {
//                 Bucket: sourceBucket,
//                 Key: file.Key
//             };
//             const data = await s3.getObject(readParams).promise();
//             const records = data.Body.toString('utf-8').split('\n');
            
//             if (!isFirstFile && records.length > 0) {
//                 // Skip the header for non-first files
//                 records.shift();
//             }
            
//             const filteredRecords = records.filter(line => line.trim() !== ''); // Remove empty lines
//             totalRecords += filteredRecords.length;
//             mergedData += filteredRecords.join('\n') + '\n';
            
//             console.log(`Appended ${filteredRecords.length} records from ${file.Key}`);
//             isFirstFile = false; // Set flag to false after processing the first file
//         }
        

//         console.log(`${files.length} files got merged`);
//         // Write the merged data to the new file
//         console.log(`Writing merged data to ${targetPrefix}/${outputFile} in ${targetBucket}`);
//         const writeParams = {
//             Bucket: targetBucket,
//             Key: `${targetPrefix}/${outputFile}`,
//             Body: mergedData
//         };
//         await s3.putObject(writeParams).promise();
//         console.log(`Successfully merged ${totalRecords} records from ${files.length} files into ${targetPrefix}/${outputFile}`);

//         res.send({ message: 'Files merged successfully', recordCount: totalRecords });
//     } catch (error) {
//         console.error('Failed to merge files:', error);
//         res.status(500).send({ error: 'Failed to merge files', details: error.message });
//     }
// });

// async function ensureFolderExists(bucket, prefix) {
//     if (!prefix.endsWith('/')) {
//         prefix += '/';
//     }
//     console.log(`Checking if folder ${prefix} exists in bucket ${bucket}`);
//     try {
//         await s3.headObject({
//             Bucket: bucket,
//             Key: prefix
//         }).promise();
//         console.log(`Folder ${prefix} exists in ${bucket}`);
//     } catch (error) {
//         if (error.code === 'NotFound') {
//             console.log(`Folder ${prefix} does not exist in ${bucket}, creating now`);
//             await s3.putObject({
//                 Bucket: bucket,
//                 Key: prefix
//             }).promise();
//             console.log(`Folder ${prefix} created in ${bucket}`);
//         } else {
//             console.error(`Error checking folder existence: ${error}`);
//             throw error;
//         }
//     }
// }

// app.listen(port, () => {
//     console.log(`Server is running on port ${port}`);
// });





/*

const express = require('express');
const aws = require('aws-sdk');
const bodyParser = require('body-parser');
const app = express();
const port = 3001;

// Load AWS credentials from a configuration file
const awsCredentials = require('../resource/awsConfig.json');

const s3 = new aws.S3({
    accessKeyId: awsCredentials.accessKeyId,
    secretAccessKey: awsCredentials.secretAccessKey,
    region: awsCredentials.region
});

app.use(bodyParser.json());

app.post('/merge-files', async (req, res) => {
    const { sourceBucket, sourcePrefix, targetBucket, targetPrefix, outputFile } = req.query;
    console.log(`Received request to merge files from ${sourcePrefix} in ${sourceBucket} to ${targetPrefix}/${outputFile} in ${targetBucket}`);

    try {
        // Check and create the target prefix if it does not exist
        await ensureFolderExists(targetBucket, targetPrefix);
        console.log(`Target prefix ${targetPrefix} verified or created in ${targetBucket}`);

        // List all files in the source prefix
        const listParams = {
            Bucket: sourceBucket,
            Prefix: sourcePrefix
        };
        console.log(`Listing files with prefix ${sourcePrefix} in bucket ${sourceBucket}`);
        const listedObjects = await s3.listObjectsV2(listParams).promise();

        const files = listedObjects.Contents.filter(item => !item.Key.endsWith('/'));
        console.log(`Found ${files.length} files to merge`);

        let mergedData = '';

        // Read each file and concatenate its contents
        for (let file of files) {
            console.log(`Reading file: ${file.Key}`);
            const readParams = {
                Bucket: sourceBucket,
                Key: file.Key
            };
            const data = await s3.getObject(readParams).promise();
            mergedData += data.Body.toString('utf-8') + '\n';
            console.log(`Appended data from ${file.Key}`);
        }

        // Write the merged data to the new file
        console.log(`Writing merged data to ${targetPrefix}/${outputFile} in ${targetBucket}`);
        const writeParams = {
            Bucket: targetBucket,
            Key: `${targetPrefix}/${outputFile}`,
            Body: mergedData
        };
        await s3.putObject(writeParams).promise();
        console.log(`Successfully merged files into ${targetPrefix}/${outputFile}`);

        res.send({ message: 'Files merged successfully' });
    } catch (error) {
        console.error('Failed to merge files:', error);
        res.status(500).send({ error: 'Failed to merge files', details: error.message });
    }
});

async function ensureFolderExists(bucket, prefix) {
    if (!prefix.endsWith('/')) {
        prefix += '/';
    }
    console.log(`Checking if folder ${prefix} exists in bucket ${bucket}`);
    try {
        await s3.headObject({
            Bucket: bucket,
            Key: prefix
        }).promise();
        console.log(`Folder ${prefix} exists in ${bucket}`);
    } catch (error) {
        if (error.code === 'NotFound') {
            console.log(`Folder ${prefix} does not exist in ${bucket}, creating now`);
            await s3.putObject({
                Bucket: bucket,
                Key: prefix
            }).promise();
            console.log(`Folder ${prefix} created in ${bucket}`);
        } else {
            console.error(`Error checking folder existence: ${error}`);
            throw error;
        }
    }
}

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});

*/
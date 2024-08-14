const express = require('express');
const csv = require('csv-parse');
const jsforce = require('jsforce');
const AWS = require('aws-sdk');
const jwt = require('jsonwebtoken');


const app = express();
app.use(express.json());


const awsCredentials = require('../resource/awsConfig.json');


AWS.config.update({
   accessKeyId: awsCredentials.accessKeyId,
   secretAccessKey: awsCredentials.secretAccessKey,
   region: awsCredentials.region
});


const s3 = new AWS.S3();
const conn = new jsforce.Connection({
   loginUrl: awsCredentials.SALESFORCE_LOGIN_URL
});


conn.login(awsCredentials.SALESFORCE_USERNAME, awsCredentials.SALESFORCE_PASSWORD + awsCredentials.SALESFORCE_TOKEN, err => {
   if (err) {
       console.error('Failed to login to Salesforce:', err);
   } else {
       console.log('Successfully logged into Salesforce.');
   }
});


// Middleware for JWT authentication
exports.authenticateJWT = (req, res, next) => {
   const authHeader = req.headers.authorization;
   if (!authHeader) {
       return res.status(401).send({ error: 'Authorization header missing' });
   }


   const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
   if (!token) {
       return res.status(401).send({ error: 'Token missing in Authorization header' });
   }


   jwt.verify(token, 'YourSecretKeyHere', (err, user) => {
       if (err) {
           return res.status(403).send({ error: 'Invalid token' });
       }
       req.user = user;
       next();
   });
};


exports.loadsqldatatosf = async (req, res) => {
   const { bucketName, folderPath, sfObjectName } = req.query;
   try {
       const params = { Bucket: bucketName, Prefix: folderPath };
       const data = await s3.listObjectsV2(params).promise();


       if (data.Contents.length === 0) {
           return res.status(404).send('No files found in the specified folder.');
       }


       const sfFields = await fetchSObjectFields(sfObjectName);
       let totalRecordsProcessed = 0;
       let totalRecordsInserted = 0;
       let allErrors = [];


       const processPromises = data.Contents.map(content =>
           processFile(bucketName, content.Key, sfObjectName, sfFields)
           .then(records => {
               totalRecordsProcessed += records.processed;
               totalRecordsInserted += records.inserted.successCount;
               allErrors.push(...records.inserted.errors);
           })
       );


       await Promise.all(processPromises);


       res.status(200).json({
           status: 'Success',
           message: 'All files processed successfully.',
           details: {
               sourceBucketName: bucketName,
               sourceFolderPath: folderPath,
               totalRecordsProcessed,
               salesforceObjectName: sfObjectName,
               totalRecordsInserted,
               errors: allErrors
           }
       });
   } catch (error) {
       console.error('Error processing files:', error);
       res.status(500).json({
           status: 'Error',
           message: 'Failed to process files.',
           errorDetails: error.message
       });
   }
};


async function fetchSObjectFields(objectName) {
   const describeResult = await conn.sobject(objectName).describe();
   return describeResult.fields.map(field => field.name);
}


async function processFile(bucketName, key, sfObjectName, sfFields) {
   const params = { Bucket: bucketName, Key: key };
   const fileStream = s3.getObject(params).createReadStream();


   const parser = fileStream.pipe(csv.parse({ columns: true, skip_empty_lines: true }));
   let data = [];
   let processed = 0;
   let inserted = 0;
   let errors = [];


   fileStream.on('error', error => console.error('Error reading from S3:', error));
   parser.on('error', error => console.error('Error parsing CSV:', error));


   for await (const row of parser) {
       const filteredRow = filterFields(row, sfFields);
       data.push(filteredRow);
       processed++;
       if (data.length >= 10000) {
           const result = await submitBatch(sfObjectName, data);
           inserted += result.successCount;
           errors.push(...result.errors);
           data = [];
       }
   }


   if (data.length > 0) {
       const result = await submitBatch(sfObjectName, data);
       inserted += result.successCount;
       errors.push(...result.errors);
   }


   console.log(`Finished processing file: ${key}`);
   return { processed, inserted: { successCount: inserted, errors } };
}


function filterFields(row, sfFields) {
   return Object.keys(row)
       .filter(key => sfFields.includes(key))
       .reduce((obj, key) => {
           obj[key] = row[key];
           return obj;
       }, {});
}


async function submitBatch(sfObjectName, data) {
   const options = {
       extIdField: 'Id',
       concurrencyMode: 'Parallel',
       allowBulkApi: true,
       pollTimeout: 600000,
       pollInterval: 10000
   };
   try {
       const results = await conn.bulk.load(sfObjectName, "insert", options, data);
       const successCount = results.filter(res => res.success).length;
       const errors = results.filter(res => !res.success).map(res => ({
           id: res.id, errors: res.errors
       }));
       return { successCount, errors };
   } catch (error) {
       console.error('Error submitting bulk job:', error);
       return { successCount: 0, errors: [{ id: 'N/A', errors: [error.message] }] };
   }
}

























// const express = require('express');
// const csv = require('csv-parse');
// const jsforce = require('jsforce');
// const AWS = require('aws-sdk');
// const jwt = require('jsonwebtoken');

// const app = express();
// app.use(express.json());

// const awsCredentials = require('../resource/awsConfig.json');

// AWS.config.update({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// const s3 = new AWS.S3();
// const conn = new jsforce.Connection({
//     loginUrl: awsCredentials.SALESFORCE_LOGIN_URL
// });


// conn.login(awsCredentials.SALESFORCE_USERNAME, awsCredentials.SALESFORCE_PASSWORD + awsCredentials.SALESFORCE_TOKEN, err => {
//     if (err) {
//         console.error('Failed to login to Salesforce:', err);
//     } else {
//         console.log('Successfully logged into Salesforce.');
//     }
// });

// // Middleware for JWT authentication
// exports.authenticateJWT = (req, res, next) => {
//     const authHeader = req.headers['authorization'];
//     if (!authHeader) {
//         return res.status(401).send({ error: 'Authorization header missing' });
//     }

//     const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
//     if (!token) {
//         return res.status(401).send({ error: 'Token missing in Authorization header' });
//     }

//     jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
//         if (err) {
//             return res.status(403).send({ error: 'Invalid token' });
//         }
//         req.user = user;
//         next();
//     });
// };


// exports.loadsqldatatosf = async (req, res) => {
//     const { bucketName, folderPath, sfObjectName } = req.query;
//     try {
//         const params = { Bucket: bucketName, Prefix: folderPath };
//         const data = await s3.listObjectsV2(params).promise();

//         if (data.Contents.length === 0) {
//             return res.status(404).send('No files found in the specified folder.');
//         }

//         const sfFields = await fetchSObjectFields(sfObjectName);
//         let totalRecordsProcessed = 0;
//         let totalRecordsInserted = 0;
//         let allErrors = [];

//         const processPromises = data.Contents.map(content =>
//             processFile(bucketName, content.Key, sfObjectName, sfFields)
//             .then(records => {
//                 totalRecordsProcessed += records.processed;
//                 totalRecordsInserted += records.inserted.successCount;
//                 allErrors.push(...records.inserted.errors);
//             })
//         );

//         await Promise.all(processPromises);

//         res.status(200).json({
//             status: 'Success',
//             message: 'All files processed successfully.',
//             details: {
//                 sourceBucketName: bucketName,
//                 sourceFolderPath: folderPath,
//                 totalRecordsProcessed,
//                 salesforceObjectName: sfObjectName,
//                 totalRecordsInserted,
//                 errors: allErrors
//             }
//         });
//     } catch (error) {
//         console.error('Error processing files:', error);
//         res.status(500).json({
//             status: 'Error',
//             message: 'Failed to process files.',
//             errorDetails: error.message
//         });
//     }
// };

// async function fetchSObjectFields(objectName) {
//     const describeResult = await conn.sobject(objectName).describe();
//     return describeResult.fields.map(field => field.name);
// }

// async function processFile(bucketName, key, sfObjectName, sfFields) {
//     const params = { Bucket: bucketName, Key: key };
//     const fileStream = s3.getObject(params).createReadStream();

//     const parser = fileStream.pipe(csv.parse({ columns: true, skip_empty_lines: true }));
//     let data = [];
//     let processed = 0;
//     let inserted = 0;
//     let errors = [];

//     fileStream.on('error', error => console.error('Error reading from S3:', error));
//     parser.on('error', error => console.error('Error parsing CSV:', error));

//     for await (const row of parser) {
//         const filteredRow = filterFields(row, sfFields);
//         data.push(filteredRow);
//         processed++;
//         if (data.length >= 10000) {
//             const result = await submitBatch(sfObjectName, data);
//             inserted += result.successCount;
//             errors.push(...result.errors);
//             data = [];
//         }
//     }

//     if (data.length > 0) {
//         const result = await submitBatch(sfObjectName, data);
//         inserted += result.successCount;
//         errors.push(...result.errors);
//     }

//     console.log(`Finished processing file: ${key}`);
//     return { processed, inserted: { successCount: inserted, errors } };
// }

// function filterFields(row, sfFields) {
//     return Object.keys(row)
//         .filter(key => sfFields.includes(key))
//         .reduce((obj, key) => {
//             obj[key] = row[key];
//             return obj;
//         }, {});
// }

// async function submitBatch(sfObjectName, data) {
//     const options = {
//         extIdField: 'Id',
//         concurrencyMode: 'Parallel',
//         allowBulkApi: true,
//         pollTimeout: 600000,
//         pollInterval: 10000
//     };
//     try {
//         const results = await conn.bulk.load(sfObjectName, "insert", options, data);
//         //const bulkv2 = await conn.bu
//         const successCount = results.filter(res => res.success).length;
//         const errors = results.filter(res => !res.success).map(res => ({
//             id: res.id, errors: res.errors
//         }));
//         return { successCount, errors };
//     } catch (error) {
//         console.error('Error submitting bulk job:', error);
//         return { successCount: 0, errors: [{ id: 'N/A', errors: [error.message] }] };
//     }
// }









// const express = require('express');
// const csv = require('csv-parse');
// const jsforce = require('jsforce');
// const AWS = require('aws-sdk');
// const jwt = require('jsonwebtoken');
// const { Transform } = require('stream');
// const { promisify } = require('util');

// const app = express();
// app.use(express.json());

// const awsCredentials = require('../resource/awsConfig.json');

// AWS.config.update({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// const s3 = new AWS.S3();
// const conn = new jsforce.Connection({
//     loginUrl: awsCredentials.SALESFORCE_LOGIN_URL
// });

// const login = promisify(conn.login.bind(conn));
// login(awsCredentials.SALESFORCE_USERNAME, awsCredentials.SALESFORCE_PASSWORD + awsCredentials.SALESFORCE_TOKEN)
//     .then(() => console.log('Successfully logged into Salesforce.'))
//     .catch(err => console.error('Failed to login to Salesforce:', err));

// exports.authenticateJWT = (req, res, next) => {
//     const authHeader = req.headers['authorization'];
//     if (!authHeader) {
//         return res.status(401).send({ error: 'Authorization header missing' });
//     }

//     const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
//     if (!token) {
//         return res.status(401).send({ error: 'Token missing in Authorization header' });
//     }

//     jwt.verify(token, 'YourSecretKeyHere', (err, user) => {
//         if (err) {
//             return res.status(403).send({ error: 'Invalid token' });
//         }
//         req.user = user;
//         next();
//     });
// };

// const processBatch = async (data, sfObjectName) => {
//     const options = {
//         extIdField: 'Id',
//         concurrencyMode: 'Parallel',
//         allowBulkApi: true,
//         pollTimeout: 600000,
//         pollInterval: 20000
//     };
//     try {
//         const result = await conn.bulk.load(sfObjectName, "insert", options, data);
//         return result.filter(res => res.success).length;
//     } catch (error) {
//         console.error('Error submitting bulk job:', error);
//         return 0;
//     }
// };

// exports.loadsqldatatosf = async (req, res) => {
//     const { bucketName, folderPath, sfObjectName } = req.query;
//     try {
//         const params = { Bucket: bucketName, Prefix: folderPath };
//         const data = await s3.listObjectsV2(params).promise();

//         if (data.Contents.length === 0) {
//             return res.status(404).send('No files found in the specified folder.');
//         }

//         const sfFields = await fetchSObjectFields(sfObjectName);
//         let totalRecordsProcessed = 0;
//         let totalRecordsInserted = 0;

//         const processPromises = data.Contents.map(content => 
//             processFile(bucketName, content.Key, sfObjectName, sfFields)
//             .then(records => {
//                 totalRecordsProcessed += records.processed;
//                 totalRecordsInserted += records.inserted;
//             })
//         );

//         await Promise.all(processPromises);

//         res.status(200).json({
//             status: 'Success',
//             message: 'All files processed successfully.',
//             details: {
//                 sourceBucketName: bucketName,
//                 sourceFolderPath: folderPath,
//                 totalRecordsProcessed,
//                 salesforceObjectName: sfObjectName,
//                 totalRecordsInserted
//             }
//         });
//     } catch (error) {
//         console.error('Error processing files:', error);
//         res.status(500).json({
//             status: 'Error',
//             message: 'Failed to process files.',
//             errorDetails: error.message
//         });
//     }
// };

// async function fetchSObjectFields(objectName) {
//     const describeResult = await conn.sobject(objectName).describe();
//     return describeResult.fields.map(field => field.name);
// }

// async function processFile(bucketName, key, sfObjectName, sfFields) {
//     const params = { Bucket: bucketName, Key: key };
//     const fileStream = s3.getObject(params).createReadStream();

//     const parser = fileStream.pipe(csv.parse({ columns: true, skip_empty_lines: true }));
//     let data = [];
//     let processed = 0;
//     let inserted = 0;

//     fileStream.on('error', error => console.error('Error reading from S3:', error));
//     parser.on('error', error => console.error('Error parsing CSV:', error));

//     for await (const row of parser) {
//         const filteredRow = filterFields(row, sfFields);
//         data.push(filteredRow);
//         processed++;
//         if (data.length >= 1000) {
//             inserted += await processBatch(data, sfObjectName);
//             data = [];
//         }
//     }
//     if (data.length > 0) {
//         inserted += await processBatch(data, sfObjectName);
//     }

//     console.log(`Finished processing file: ${key}`);
//     return { processed, inserted };
// }

// function filterFields(row, sfFields) {
//     return Object.keys(row)
//         .filter(key => sfFields.includes(key))
//         .reduce((obj, key) => {
//             obj[key] = row[key];
//             return obj;
//         }, {});
// }





// const express = require('express');
// const csv = require('csv-parse');
// const jsforce = require('jsforce');
// const AWS = require('aws-sdk');
// const jwt = require('jsonwebtoken');
// const { Transform } = require('stream');
// const { promisify } = require('util');

// const app = express();
// app.use(express.json());

// const awsCredentials = require('../resource/awsConfig.json');

// AWS.config.update({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
// });

// const s3 = new AWS.S3();
// const conn = new jsforce.Connection({
//     loginUrl: awsCredentials.SALESFORCE_LOGIN_URL
// });

// const login = promisify(conn.login.bind(conn));
// login(awsCredentials.SALESFORCE_USERNAME, awsCredentials.SALESFORCE_PASSWORD + awsCredentials.SALESFORCE_TOKEN)
//     .then(() => console.log('Successfully logged into Salesforce.'))
//     .catch(err => console.error('Failed to login to Salesforce:', err));

// exports.authenticateJWT = (req, res, next) => {
//     const authHeader = req.headers['authorization'];
//     if (!authHeader) {
//         return res.status(401).send({ error: 'Authorization header missing' });
//     }

//     const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;
//     if (!token) {
//         return res.status(401).send({ error: 'Token missing in Authorization header' });
//     }

//     jwt.verify(token, 'YourSecretKeyHere', (err, user) => {
//         if (err) {
//             return res.status(403).send({ error: 'Invalid token' });
//         }
//         req.user = user;
//         next();
//     });
// };

// const processBatch = async (data, sfObjectName) => {
//     const options = {
//         extIdField: 'Id',
//         concurrencyMode: 'Parallel',
//         allowBulkApi: true,
//         pollTimeout: 600000,
//         pollInterval: 20000
//     };
//     try {
//         const result = await conn.bulk.load(sfObjectName, "insert", options, data);
//         return result.filter(res => res.success).length;
//     } catch (error) {
//         console.error('Error submitting bulk job:', error);
//         return 0;
//     }
// };

// exports.loadsqldatatosf = async (req, res) => {
//     const { bucketName, folderPath, sfObjectName } = req.query;
//     try {
//         const params = { Bucket: bucketName, Prefix: folderPath };
//         const data = await s3.listObjectsV2(params).promise();

//         if (data.Contents.length === 0) {
//             return res.status(404).send('No files found in the specified folder.');
//         }

//         const sfFields = await fetchSObjectFields(sfObjectName);
//         let totalRecordsProcessed = 0;
//         let totalRecordsInserted = 0;

//         const processPromises = data.Contents.map(content => 
//             processFile(bucketName, content.Key, sfObjectName, sfFields)
//             .then(records => {
//                 totalRecordsProcessed += records.processed;
//                 totalRecordsInserted += records.inserted;
//             })
//         );

//         await Promise.all(processPromises);

//         res.status(200).json({
//             status: 'Success',
//             message: 'All files processed successfully.',
//             details: {
//                 sourceBucketName: bucketName,
//                 sourceFolderPath: folderPath,
//                 totalRecordsProcessed,
//                 salesforceObjectName: sfObjectName,
//                 totalRecordsInserted
//             }
//         });
//     } catch (error) {
//         console.error('Error processing files:', error);
//         res.status(500).json({
//             status: 'Error',
//             message: 'Failed to process files.',
//             errorDetails: error.message
//         });
//     }
// };

// async function fetchSObjectFields(objectName) {
//     const describeResult = await conn.sobject(objectName).describe();
//     return describeResult.fields.map(field => field.name);
// }

// async function processFile(bucketName, key, sfObjectName, sfFields) {
//     const params = { Bucket: bucketName, Key: key };
//     const fileStream = s3.getObject(params).createReadStream();

//     const parser = fileStream.pipe(csv.parse({ columns: true, skip_empty_lines: true }));
//     let data = [];
//     let processed = 0;
//     let inserted = 0;

//     fileStream.on('error', error => console.error('Error reading from S3:', error));
//     parser.on('error', error => console.error('Error parsing CSV:', error));

//     for await (const row of parser) {
//         const filteredRow = filterFields(row, sfFields);
//         data.push(filteredRow);
//         processed++;
//         if (data.length >= 1000) {
//             inserted += await processBatch(data, sfObjectName);
//             data = [];
//         }
//     }
//     if (data.length > 0) {
//         inserted += await processBatch(data, sfObjectName);
//     }

//     console.log(`Finished processing file: ${key}`);
//     return { processed, inserted };
// }

// function filterFields(row, sfFields) {
//     return Object.keys(row)
//         .filter(key => sfFields.includes(key))
//         .reduce((obj, key) => {
//             obj[key] = row[key];
//             return obj;
//         }, {});
// }








// app.listen(3000, () => {
//     console.log('Server is running on port 3000');
// });



// const express = require('express');
// const csv = require('csv-parse');
// const jsforce = require('jsforce');
// const AWS = require('aws-sdk');

// const app = express();
// app.use(express.json());
// const awsCredentials = require('../resource/awsConfig.json');
// const jwt = require('jsonwebtoken');


// AWS.config.update({
//     accessKeyId: awsCredentials.accessKeyId,
//     secretAccessKey: awsCredentials.secretAccessKey,
//     region: awsCredentials.region
//   });

// const s3 = new AWS.S3();

// const salesforceUsername = awsCredentials.SALESFORCE_USERNAME;
// const salesforcePassword = awsCredentials.SALESFORCE_PASSWORD;
// const salesforceToken = awsCredentials.SALESFORCE_TOKEN;
// const salesforceLoginUrl = awsCredentials.SALESFORCE_LOGIN_URL;

// const conn = new jsforce.Connection({
//     loginUrl: salesforceLoginUrl
// });

// conn.login(salesforceUsername, salesforcePassword + salesforceToken, err => {
//     if (err) {
//         console.error('Failed to login to Salesforce:', err);
//         return;
//     }
//     console.log('Successfully logged into Salesforce.');
// });

// exports.authenticateJWT = (req, res, next) => {
//     console.log('Request headers:', req.headers); 
//     const authHeader = req.headers['authorization']
//     if (!authHeader) {
//         console.log('Authorization header missing');
//         return res.status(401).send({ error: 'Authorization header missing' });
//     }
//     console.log('Header', authHeader);

//     const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;

//     if (!token) {
//         console.log('Token missing in Authorization header');
//         return res.status(401).send({ error: 'Token missing in Authorization header' });
//     }

//     jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
//         if (err) {
//             console.log('Token verification failed:', err.message);
//             return res.status(403).send({ error: 'Invalid token' });
//         }
//         req.user = user;
//         next();
//     });
// };


// exports.loadsqldatatosf = async (req, res) => {
// //app.post('/loadsqldatatosf', async (req, res) => {
//     const { bucketName, folderPath, sfObjectName } = req.query;
//     let totalRecordsProcessed = 0;
//     let totalRecordsInserted = 0;

//     try { 
//         const params = {
//             Bucket: bucketName,
//             Prefix: folderPath
//         };

//         const data = await s3.listObjectsV2(params).promise();

//         if (!data.Contents.length) {
//             return res.status(404).send('No files found in the specified folder.');
//         }

//         const sfFields = await fetchSObjectFields(sfObjectName);

//         for (let content of data.Contents) {
//             const records = await processFile(bucketName, content.Key, sfObjectName,sfFields);
//             totalRecordsProcessed += records.processed;
//             totalRecordsInserted += records.inserted;
//         }

//         res.status(200).json({
//             status: 'Success',
//             message: 'All files processed successfully.',
//             details: {
//                 sourceBucketName: bucketName,
//                 sourceFolderPath: folderPath,
//                 totalRecordsProcessed: totalRecordsProcessed,
//                 salesforceObjectName: sfObjectName,
//                 totalRecordsInserted: totalRecordsInserted
//             }
//         });
//     } catch (error) {
//         console.error('Error processing files:', error);
//         res.status(500).json({
//             status: 'Error',
//             message: 'Failed to process files.',
//             errorDetails: error.message
//         });
//     }
// };

// async function fetchSObjectFields(objectName) {
//     try {
//         const describeResult = await conn.sobject(objectName).describe();
//         return describeResult.fields.map(field => field.name);
//     } catch (err) {
//         console.error('Failed to fetch Salesforce object fields:', err);
//         throw err;
//     }
// }

// async function processFile(bucketName, key, sfObjectName,sfFields) {
//     const params = {
//         Bucket: bucketName,
//         Key: key
//     };

//     const fileStream = s3.getObject(params).createReadStream();
//     const parser = fileStream.pipe(csv.parse({ columns: true, skip_empty_lines: true }));
//     let data = [];
//     let processed = 0;
//     let inserted = 0;

//     return new Promise((resolve, reject) => {
//         parser.on('data', (row) => {
//             const filteredRow = Object.keys(row)
//                 .filter(key => sfFields.includes(key))
//                 .reduce((obj, key) => {
//                     obj[key] = row[key];
//                     return obj;
//                 }, {});

//             data.push(filteredRow);
//             processed++;

//             if (data.length >= 10000) {
//                 inserted += data.length;
//                 submitBatch(data);
//                 data = [];
//             }
//         });

//         parser.on('end', () => {
//             if (data.length > 0) {
//                 inserted += data.length;
//                 submitBatch(sfObjectName,data);
//             }
//             console.log(`Finished processing file: ${key}`);
//             resolve({ processed, inserted });
//         });

//         parser.on('error', (err) => {
//             console.error('Error parsing CSV:', err);
//             reject(err);
//         });
//     });
// }

// function submitBatch(sfObjectName,data) {

//     const options = {
//         extIdField: 'Id', 
//         concurrencyMode: 'Parallel',
//         pollTimeout: 600000, 
//         pollInterval: 20000  
//     };
//    // conn.bulk.load("SampleLoad__c", "insert", data, (err, result) => {

//     conn.bulk.load(sfObjectName, "insert", data, (err, result) => { sfObjectName
//         if (err) {
//             console.error('Error submitting bulk job:', err);
//             return;
//         }
//         result.forEach((res) => {
//             if (!res.success) {
//                 console.log('Failed to insert record:', res);
//             }
//         });
//     });
// }




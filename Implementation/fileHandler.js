const multer = require('multer');
const fs = require('fs');
const { S3Client, PutObjectCommand, CopyObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { GlueClient, StartCrawlerCommand, GetCrawlerCommand } = require('@aws-sdk/client-glue');
const jwt = require('jsonwebtoken');
const { secretKey } = require('./jsonwebtoken');
const upload = multer({ dest: 'uploads/' });
const awsCredentials = require('../resource/awsConfig.json');
const s3Client = new S3Client({ credentials: awsCredentials,region: 'us-east-1' });
const glueClient = new GlueClient({ credentials: awsCredentials,region: 'us-east-1'});
const { generateErrorResponse,generateSuccessResponse } = require('./errorResponse');


console.log("Hello...");
exports.multerMiddleware = (req, res, next) => {
    upload.single('file')(req, res, function (err) {
        if (err instanceof multer.MulterError) {
            return res.status(500).send({ error: "Multer error: " + err.message });
        } else if (err) {
            return res.status(500).send({ error: "General error: " + err.message });
        }
        if (!req.file) {
            return res.status(400).send({ error: 'No file uploaded.' });
        }
        next(); 
    });
};


exports.authenticateJWT = (req, res, next) => {
    console.log('Request headers:', req.headers); 
    const authHeader = req.headers['authorization'];
    if (!authHeader) {
        console.log('Authorization header missing');
        return res.status(401).send({ error: 'Authorization header missing' });
    }
    console.log('Header', authHeader);

    const token = authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : authHeader;

    if (!token) {
        console.log('Token missing in Authorization header');
        return res.status(401).send({ error: 'Token missing in Authorization header' });
    }

    jwt.verify(token, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', (err, user) => {
        if (err) {
            console.log('Token verification failed:', err.message);
            return res.status(403).send({ error: 'Invalid token' });
        }
        req.user = user;
        next();
    });
};




console.log("Hello World")


exports.uploadFile = async (req, res) => {
    const file = req.file;
    const fileStream = fs.createReadStream(file.path);
    const bucketName = req.query.bucketName
    const crawlerName = req.query.crawlerName

    let missing = [];
    if (!bucketName) missing.push('Bucket Name');
    if (!crawlerName) missing.push('Crawler Name');

    if (missing.length > 0) {
        return res.status(400).send(generateErrorResponse(missing));
    }

    console.log('****************************')
    console.log(file.originalname)
    console.log('****************************')

    const uploadParams = { Bucket: bucketName, Key: `New/${file.originalname}`, Body: fileStream };
    

    try {
        await s3Client.send(new PutObjectCommand(uploadParams));
        await glueClient.send(new StartCrawlerCommand({ Name: crawlerName }));


        let isCrawlerRunning = true;
        while (isCrawlerRunning) {
            await new Promise(resolve => setTimeout(resolve, 60000)); // Wait for 60 seconds
            const { Crawler } = await glueClient.send(new GetCrawlerCommand({ Name: crawlerName }));
            isCrawlerRunning = Crawler.State === 'RUNNING';
        }

        //await s3Client.send(new CopyObjectCommand({ Bucket: uploadParams.Bucket, CopySource: `${uploadParams.Bucket}/New/${file.originalname}`, Key: `Crawled/${file.originalname}` }));
        //await s3Client.send(new DeleteObjectCommand({ Bucket: uploadParams.Bucket, Key: uploadParams.Key }));

        //res.send('File uploaded, Glue Crawler started, and file moved after crawling completed.');
        const message='File uploaded into the S3, Glue Crawler started, and file moved to another location after crawler completes its Job.'
        res.status(200).send(generateSuccessResponse(message));
    } catch (err) {
        console.error('Error:', err);
        res.status(500).send({ error: err.message });
    } finally {
        fs.unlink(file.path, err => {
            if (err) console.error("Error deleting local file:", err);
        });
    }
};


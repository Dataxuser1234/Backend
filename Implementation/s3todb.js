const AWS = require('aws-sdk');
const fs = require('fs');
const csv = require('csv-parser');

// Configure  AWS SDK
AWS.config.update({
  region: 'us-west-2', 
  accessKeyId: 'your-access-key-id', 
  secretAccessKey: 'your-secret-access-key'
});

const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

// determine the schema from the CSV file
const getSchemaFromCSV = (csvFilePath) => {
  return new Promise((resolve, reject) => {
    const headers = [];
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('headers', (headerList) => {
        headerList.forEach(header => {
          headers.push({ AttributeName: header, AttributeType: 'S' }); // Assume all attributes are strings for simplicity
        });
        resolve(headers);
      })
      .on('error', (err) => {
        reject(err);
      });
  });
};

// create a DynamoDB table
const createDynamoDBTable = async (tableName, schema) => {
  const params = {
    TableName: tableName,
    KeySchema: [
      { AttributeName: schema[0].AttributeName, KeyType: 'HASH' } // Assuming the first attribute as the primary key
    ],
    AttributeDefinitions: schema,
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5
    }
  };

  try {
    await dynamodb.createTable(params).promise();
    console.log(`Table "${tableName}" created successfully.`);
  } catch (err) {
    console.error(`Error creating table: ${err}`);
  }
};

// read data from CSV and upload to DynamoDB
const loadCSVToDynamoDB = async (csvFilePath, tableName) => {
  const items = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', (row) => {
      items.push(row);
    })
    .on('end', async () => {
      console.log('CSV file successfully processed');
      for (const item of items) {
        const params = {
          TableName: tableName,
          Item: item,
        };

        try {
          await docClient.put(params).promise();
          console.log(`Inserted item: ${JSON.stringify(item)}`);
        } catch (err) {
          console.error(`Unable to insert item. Error JSON: ${JSON.stringify(err, null, 2)}`);
        }
      }
      console.log('All items have been processed.');
    });
};

// Main function to create table and load data
const main = async (csvFilePath, tableName) => {
  try {
    const schema = await getSchemaFromCSV(csvFilePath);
    await createDynamoDBTable(tableName, schema);
    console.log('Waiting for table to be active...');
    await dynamodb.waitFor('tableExists', { TableName: tableName }).promise();
    await loadCSVToDynamoDB(csvFilePath, tableName);
  } catch (err) {
    console.error(`Error: ${err}`);
  }
};

// Replace 'your-file-path.csv' and 'YourDynamoDBTableName' with your CSV file path and desired DynamoDB table name
main('your-file-path.csv', 'YourDynamoDBTableName');

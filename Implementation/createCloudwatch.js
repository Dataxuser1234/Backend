const AWS = require('aws-sdk');
const logGroupName = '/load-into-salesforce';

const awsCredentials = require('../resource/awsConfig.json');



AWS.config.update({
    region: awsCredentials.region,
    credentials: {
        accessKeyId: awsCredentials.accessKeyId,
        secretAccessKey: awsCredentials.secretAccessKey
    }
});

const cwLogs = new AWS.CloudWatchLogs();
const logStreamName = `logStream-${new Date().toISOString().split('T')[0]}`; // Daily log stream

async function ensureLogInfrastructure() {
    try {
      await cwLogs.createLogGroup({ logGroupName }).promise();
      console.log('Log group created:', logGroupName);
    } catch (error) {
      if (error.code !== 'ResourceAlreadyExistsException') {
        console.error('Failed to create log group:', error);
        return;
      }
      console.log('Log group already exists:', logGroupName);
    }
  
    try {
      await cwLogs.createLogStream({ logGroupName, logStreamName }).promise();
      console.log('Log stream created:', logStreamName);
    } catch (error) {
      if (error.code !== 'ResourceAlreadyExistsException') {
        console.error('Failed to create log stream:', error);
        return;
      }
      console.log('Log stream already exists:', logStreamName);
    }
  }
  

  let sequenceToken = null; 
  async function logToCloudWatch(message, level, data = {}) {
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
      console.log('Log event sent to CloudWatch');
    } catch (error) {
      if (error.code === 'InvalidSequenceTokenException') {
        sequenceToken = error.message.match(/sequenceToken is: (.*?) /)[1];
        await logToCloudWatch(message, level, data); 
      } else {
        console.error('Failed to log event:', error);
      }
    }
  }
  
  // Main function to setup logging and run the application
  async function main() {
    await ensureLogInfrastructure();
    await logToCloudWatch('Application has started', 'info', { detail: 'More details about the application startup' });
  }
  
  main();
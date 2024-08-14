// import express from 'express';
const express = require('express');
const app = express();
app.use(express.json());
// import { DataBrewClient, CreateRecipeCommand, CreateProjectCommand, CreateRecipeJobCommand } from '@aws-sdk/client-databrew'
const { DataBrewClient, 
    CreateRecipeCommand, 
    CreateProjectCommand, 
    CreateRecipeJobCommand, 
    CreateDatasetCommand, 
    PublishRecipeCommand, 
    StartJobRunCommand, 
    DescribeJobRunCommand,
    ListRecipesCommand,
    ListDatasetsCommand,
    ListProjectsCommand,
    ListJobsCommand  } = require('@aws-sdk/client-databrew');
const constants = require('../constants/brewConst.json');
const awsCredentials = require('../resource/awsConfig.json');


// import dotenv from 'dotenv';
const dotenv = require('dotenv').config();
const fs = require('fs');





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





// Initialize the DataBrew client using environment variables
const databrewClient = new DataBrewClient({
    region: awsCredentials.region,
    credentials: {
      accessKeyId: awsCredentials.accessKeyId,
      secretAccessKey: awsCredentials.secretAccessKey,
      roleArn: awsCredentials.Role
    }
  });

  async function checkResourceExists(name, type) {
    const commands = {
        'recipe': new ListRecipesCommand({}),
        'dataset': new ListDatasetsCommand({}),
        'project': new ListProjectsCommand({}),
        'job': new ListJobsCommand({})
    };
    try {
        const response = await databrewClient.send(commands[type]);
        return response[type + 's'] ? response[type + 's'].some(resource => resource.Name === name) : false;
    } catch (error) {
        console.error(`Error checking if ${type} exists:`, error);
        return false;
    }
}

// create input operarions array
function createOperation(operation){
  operation.id = operation.name + '_operation';
  operation.sourceColumns = JSON.stringify(operation.sourceColumns);
  operation.valueExpression = createCasevalueExpression(operation.conditionsMap);
  operation.withExpressions= "[]";
  return operation;
}




// function to create switch case statement from the condtions provided by user. we will use this string statemnet in recipe creation
function createCasevalueExpression(conditionsMap){
      let valueExpression = 'case ';
      conditionsMap?.conditions?.forEach(condition => {
          valueExpression += `when \`${condition.sourceColumn}\` ${condition.logicalOperator} ${condition.value} then '${condition.result}' `;
      })
      valueExpression += `else '${conditionsMap?.defaultresult}' end`;
      console.log('valueExpression ',valueExpression);
      return valueExpression;
}








function createRecipe(recipeName,operationsArray){
  return new CreateRecipeCommand({
      Name: recipeName,
      Steps: createRecipeSteps(operationsArray)
   });
}




function createRecipeSteps(operationsArray){
  const recipeStepsArray=[];
  operationsArray.forEach(operation => {
      const recipeAction = {
          Action: {
              Operation: operation.name,
              Parameters: createRecipeOperationParams(operation)
           }
      }
      recipeStepsArray.push(recipeAction);
  });
  console.log('recipeStepsArray ',recipeStepsArray);
  return recipeStepsArray;
}




// function to return Parameters of a corresponding trnasform operation
function createRecipeOperationParams(operation){
  if(operation.name == 'CASE_OPERATION'){
      return {
          functionStepType : operation.functionStepType,
          targetColumn: operation.targetColumn,
          valueExpression: operation.valueExpression,
          withExpressions: operation.withExpressions
       }
  }
  else if(operation.name == 'MERGE'){
      return {
          sourceColumns : operation.sourceColumns,
          delimiter: operation.delimiter,
          targetColumn: operation.targetColumn
       }
  }
  /*else if(operation.name == 'RENAME'){
      return {
          sourceColumn : operation.sourceColumn,
          targetColumn: operation.targetColumn
       }
  }
  else if(operation.name == 'DUPLICATE'){
      return {
          sourceColumn : operation.sourceColumn,
          targetColumn: operation.targetColumn
       }
  }
  */
  else if(operation.name == 'RENAME' || operation.name == 'DUPLICATE'){
      return {
          sourceColumn : operation.sourceColumn,
          targetColumn: operation.targetColumn
       }
  }
  else if(operation.name == 'FORMAT_DATE'){
      return {
          sourceColumn : operation.sourceColumn,
          targetDateFormat: operation.targetDateFormat
       }
  }
  else if(operation.name == 'DELETE'){
    return {
        sourceColumns : operation.sourceColumns,
    }
}
else if(operation.name == 'SPLIT_COLUMN_SINGLE_DELIMITER'){
    return {
        includeInSplit : operation.includeInSplit,
        limit: operation.limit,
        pattern: operation.pattern,
        sourceColumn: operation.sourceColumn,
    }
}
}


// function to create a datset using dataset params with mysql as data source
function createDataset(datasetParams){
   const datasetInput = {
       Name: datasetParams.datasetName,
       Input: {
           DatabaseInputDefinition: {
               GlueConnectionName: datasetParams.glueConnectionName,
               DatabaseTableName: datasetParams.databaseTableName,
             }
       }
     };
   return new CreateDatasetCommand(datasetInput);
}




//function to create project using appropriate params
function createProject(projectName,recipeName,datasetParams,roleArn){
  return new CreateProjectCommand({
      Name: projectName,
      RecipeName: recipeName,
      DatasetName: datasetParams.datasetName,
      RoleArn: roleArn,
  });
}




//function to create brew Job using appropriate params
// function createDataBrewJob(jobParams,recipeName,datasetParams,roleArn){  //creating project so no need of recipe and datset
function createDataBrewJob(jobParams,projectName,roleArn){
  // Create a DataBrew job
  return new CreateRecipeJobCommand({
      Name: jobParams.jobName,
      ProjectName: projectName,
      /* we should pass RecipeReference and DatasetName if we want to create Job without creating project and also prior to this we need to publish the recipe
      RecipeReference: {
             Name: recipeName,
             RecipeVersion: '1.0'
      },
      DatasetName : datasetParams.datasetName,
      */
      RoleArn: roleArn,
      Outputs: [
          {
              Location: {
                  Bucket: jobParams.s3OutputBucketName,
                  Key: jobParams.s3OutputPath
              },
              Format: jobParams.outputFileFormat
          }
      ]
     });
}




function describeJob(jobParams,statrJobResponse){
  return new DescribeJobRunCommand(
      {
      Name: jobParams.jobName,
      RunId: statrJobResponse.RunId,
      }
  );
}




function displayBrewJobStatus(status,intervalId){
  if (status === "RUNNING") {
      console.log("Job is still running...");
    } else if (status === "SUCCEEDED") {
      console.log("Job completed successfully.");
      clearInterval(intervalId); // Stop the interval
    } else if (status === "FAILED") {
      console.log("Job encountered an error.");
      clearInterval(intervalId); // Stop the interval
    } else {
      console.log("Unknown status:", status);
      clearInterval(intervalId); // Stop the interval
    }
}






// the start point
app.post('/create-and-run-brew', async (req, res) => {
    const { recipeName, datasetParams, projectName, jobParams, operationsArray } = req.body;
 try {

    if (!recipeName || !datasetParams || !projectName || !jobParams || !operationsArray) {
        return res.status(400).send({ error: 'Missing required fields' });
    }

    const resourcesExist = await Promise.all([
        checkResourceExists(recipeName, 'recipe'),
        checkResourceExists(datasetParams.datasetName, 'dataset'),
        checkResourceExists(projectName, 'project'),
        checkResourceExists(jobParams.jobName, 'job')
    ]);

    if (resourcesExist.some(exists => exists)) {
        return res.status(400).send({ error: 'One or more resources already exist' });
    }

  const requestBody = req.body;
  const input_operation_array=[];
  requestBody.operationsArray.forEach( operation => {
  input_operation_array.push(createOperation(operation));
  });


  // Create a DataBrew recipe
  // const createRecipeCommand = createRecipe(recipeName,operationsArray);
  const createRecipeCommand = createRecipe(requestBody.recipeName,input_operation_array);
  const recipe = JSON.stringify(createRecipeCommand);
  const recipeResponse = await databrewClient.send(createRecipeCommand);
  console.log(`Recipe created: ${recipeResponse.Name}`);
  console.log('Recipe Response : ',recipeResponse);




  // using PublishRecipeCommand we need to publish the recipe if we want to use recipe direclty in job creation without creating project
  // const publishRecipeResponse = await databrewClient.send(new PublishRecipeCommand({ Name: recipeName }));




  // Create Dataset
  const createDataSetCommand = createDataset(requestBody.datasetParams);
  const datasetResponse = await databrewClient.send(createDataSetCommand);
  console.log('Dataset Created');
  console.log('Dataset Response: ',datasetResponse);
   // Create a DataBrew project
  const createProjectCommand = createProject(requestBody.projectName,requestBody.recipeName,requestBody.datasetParams,requestBody.roleArn);
  const projectResponse = await databrewClient.send(createProjectCommand);
  console.log(`Project created: ${projectResponse.Name}`);
  console.log('Project Response : ',projectResponse);




  // create DataBrewJob
  // const createRecipeJobCommand = createDataBrewJob(requestBody.jobParams,requestBody.recipeName,requestBody.datasetParams,requestBody.roleArn);
  const createRecipeJobCommand = createDataBrewJob(requestBody.jobParams,requestBody.projectName,requestBody.roleArn);
  const jobResponse = await databrewClient.send(createRecipeJobCommand);
  console.log(`Job created: ${jobResponse.Name}`);
  console.log('Job Response : ',jobResponse);




  // run DataBrewJob
  const startJobResponse = await databrewClient.send(new StartJobRunCommand({ Name : requestBody.jobParams.jobName}));
  console.log('startJobResponse ',startJobResponse);




  // check DataBrewJob Status
  let intervalId;
  intervalId = setInterval(async () => {
      const describeResponse = await databrewClient.send( describeJob(requestBody.jobParams,startJobResponse) );
      const status = describeResponse.State;
      // console.log(`Job status: ${status}`);
      displayBrewJobStatus(status,intervalId);
  }, 5000); // Check status every 5 seconds (adjust as needed)




  // res.status(200).send(`Job created: ${jobResponse.Name}`);




  // Write the brew recipe to the file just to verify
  filePath = process.cwd()+'/brew_recipe.json';
  console.log('filePath ',filePath);
  fs.writeFile(filePath,recipe , (err) => {
  if (err) {
      console.error('Error writing to file', err);
  } else {
      console.log(`Script has been saved to ${filePath}`);
  }
  });
  res.json(createRecipeCommand);
  } catch (error) {
     console.error('Entire error:', error);
     console.error('Error message:', error.message);
     res.status(500).send(`An error occurred. ${error.message}`);
   //   res.status(500).send(`An error occurred ${req.body}`);
  //    res.status(500).send('An error occurred');
 } finally{
  if(server != null){
    server.close(() => {
      console.log('Server has been stopped');
    });
  }
}
});








// Start the server
const port = 3003;
const server = app.listen(port, () => {
console.log(`Server is running on http://localhost:${port}`);
});


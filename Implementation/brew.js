// import express from 'express';
const express = require('express');
const app = express();
app.use(express.json());

// import { DataBrewClient, CreateRecipeCommand, CreateProjectCommand, CreateRecipeJobCommand } from '@aws-sdk/client-databrew'
const { DataBrewClient, CreateRecipeCommand, CreateProjectCommand, CreateRecipeJobCommand, CreateDatasetCommand, StartJobRunCommand, DescribeJobRunCommand  } = require('@aws-sdk/client-databrew');


const fs = require('fs');

const awsCredentials = require('../resource/awsConfig.json');



// Initialize the DataBrew client using environment variables
const databrewClient = new DataBrewClient({
   region: awsCredentials.region,
   credentials: {
     accessKeyId: awsCredentials.accessKeyId,
     secretAccessKey: awsCredentials.secretAccessKey,
   }
 });





// Parameters
const recipeName = 'multiple-operations-recipe-test-postman-input2'; //
const projectName = 'multiple-operations-project-test-postman-input2'; //
const datasetParams = {
   datasetName : 'multiple-operations-resolution-dataset-postman-input2', //
   datasetFormat : 'CSV', //
   datasetDelimiter : ',', //
   datasetContainsHeaderRow : true,
   datasetS3BucketName : 'sftp-s3-home', //
   datasetS3InputKey : 'resolution.csv' //
}
const jobParams = {
   jobName : 'multiple-columns-job-postman-input2', //
   s3OutputBucketName : 'sftp-s3-home', //
   s3OutputPath : 'brew-output/multiple-operations-job/', //
   outputFileFormat : 'CSV' //


}
const roleArn = awsCredentials.Role; 


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
}


// function to create a datset using dataset params
function createDataset(datasetParams){
   const datasetInput = {
       Name: datasetParams.datasetName,
       Format: datasetParams.datasetFormat,
       FormatOptions : createDatasetFormatOptions(datasetParams),
       // similar to formatOptions need to implement below Input for s3, datacatalogue and other
       Input: {
         S3InputDefinition: {
           Bucket: datasetParams.datasetS3BucketName,
           Key: datasetParams.datasetS3InputKey,
         }
       }
       // Input : createDatasetinput(datasetParams) need to be impelmented
     };
   return new CreateDatasetCommand(datasetInput);
}


//function to create a FormatOptions of dataset input based on the input data format
function createDatasetFormatOptions(datasetParams){
   if(datasetParams.datasetFormat == 'CSV' ){
       return {
           Csv: {
               Delimiter: datasetParams.datasetDelimiter,
               HeaderRow: datasetParams.datasetContainsHeaderRow ,
             },
       }
   }
   // JSON and EXCEL need to be implemented
   else if(datasetParams.datasetFormat == 'JSON'){
       return {
           Json: { // JsonOptions
               MultiLine: true || false,
           }
       }
   }
   else if(datasetParams.datasetFormat == 'EXCEL'){
     return {
       Excel: { // ExcelOptions
           SheetNames: [ // SheetNameList
           "STRING_VALUE",
           ],
           SheetIndexes: [ // SheetIndexList
           Number("int"),
           ],
           HeaderRow: true || false,
           },
       }
   }  
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
function createDataBrewJob(jobParams,recipeName,datasetParams,roleArn){
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


// create input array
function createOperation(operation){
   operation.id = operation.name + '_operation';
   operation.sourceColumns = JSON.stringify(operation.sourceColumns);
   operation.valueExpression = createCasevalueExpression(operation.conditionsMap);
   operation.withExpressions= "[]";
   return operation;
}


// the start point
app.post('/create-and-run-brew', async (req, res) => {
  try {
   // Create a DataBrew recipe
   const input_operation_array=[];
   req.body.forEach( operation => {
       input_operation_array.push(createOperation(operation));
   });
   // const createRecipeCommand = createRecipe(recipeName,operationsArray);
   const createRecipeCommand = createRecipe(recipeName,input_operation_array);
   const recipe = JSON.stringify(createRecipeCommand);
   const recipeResponse = await databrewClient.send(createRecipeCommand);
   console.log(`Recipe created: ${recipeResponse.Name}`);


   // using PublishRecipeCommand we need to publish the recipe if we want to use recipe direclty in job creation without creating project
   // const publishRecipeResponse = await databrewClient.send(new PublishRecipeCommand({ Name: recipeName }));


   // Create Dataset
   const createDataSetCommand = createDataset(datasetParams);
   const datasetResponse = await databrewClient.send(createDataSetCommand);
   console.log('Dataset created: ',datasetResponse);
  
   // Create a DataBrew project
   const createProjectCommand = createProject(projectName,recipeName,datasetParams,roleArn);
   const projectResponse = await databrewClient.send(createProjectCommand);
   console.log(`Project created: ${projectResponse.Name}`);


   // create DataBrewJob
   const createRecipeJobCommand = createDataBrewJob(jobParams,recipeName,datasetParams,roleArn);
   const jobResponse = await databrewClient.send(createRecipeJobCommand);
   console.log(`Job created: ${jobResponse.Name}`);


   // run DataBrewJob
   const statrJobResponse = await databrewClient.send(new StartJobRunCommand({ Name : jobParams.jobName}));
   console.log('statrJobResponse ',statrJobResponse);


   // check DataBrewJob Status
   let intervalId;
   intervalId = setInterval(async () => {
       const describeResponse = await databrewClient.send( describeJob(jobParams,statrJobResponse) );
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
      console.error('Error:', error);
      res.status(500).send(`An error occurred ${req.body}`);
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
const port = 3000;
const server = app.listen(port, () => {
 console.log(`Server is running on http://localhost:${port}`);
});




// POST CAll From POSTMAN:

// We do need to start the server first by executing node fileName.js, then we do need to do a post request from the postman to the below URL.
// http://localhost:3000/create-and-run-brew

// Postman body can be either a single operation or array of multiple operations. 

// Examples:
// Postman Body Input Array which has multiple operations:

// [
//   {
//   "name": "CASE_OPERATION",
//   "functionStepType": "CASE_OPERATION",
//   "targetColumn": "Experience",
//   "conditionsMap": {
//       "conditions":[
//           {
//           "sourceColumn" : "vote_id",
//           "logicalOperator" : "<",
//           "value" : 150,
//           "result" : "Beginners"
//           },
//           {
//           "sourceColumn" : "vote_id",
//           "logicalOperator" : ">=",
//           "value" : 350,
//           "result" : "Advance"
//           }
//           ],
//       "defaultresult" : "Intermediate"
//       }
//   },
//   {
//   "name": "RENAME",
//   "sourceColumn": "economic_development",
//   "targetColumn": "ecoDevlopment"
//   },
//   {
//   "name": "MERGE",
//   "delimiter": "_",
//   "sourceColumns": ["resolution","assembly_session"],
//   "targetColumn": "Merged column 1"
//   }
// ]





// Postman Body Input Array which has single operation(MERGE):

// [{
//    "name": "MERGE",
//    "delimiter": "_",
//    "sourceColumns": ["resolution","assembly_session"],
//    "targetColumn": "Merged column 1"
// }]

// CASE_OPERATION for picklist scenario. 
// RENAME can be used for 1:1 field mapping.
// MERGE operation can be used for concating two columns.












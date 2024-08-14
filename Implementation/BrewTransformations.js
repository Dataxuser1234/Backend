const express = require('express');
const { DataBrewClient, CreateRecipeCommand, CreateProjectCommand, CreateRecipeJobCommand, CreateDatasetCommand, ListRecipesCommand, ListDatasetsCommand, ListProjectsCommand, ListJobsCommand, StartJobRunCommand, DescribeJobRunCommand } = require('@aws-sdk/client-databrew');
const dotenv = require('dotenv').config();
const fs = require('fs');
const jwt = require('jsonwebtoken');


const constants = require('../constants/brewConst.json');
const awsCredentials = require('../resource/awsConfig.json');

const app = express();
app.use(express.json());


exports.authenticateJWT = (req, res, next) => {
    console.log('Request headers:', req.headers); 
    const authHeader = req.headers['authorization']
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


function createOperation(operation) {
    operation.id = operation.name + '_operation';
    operation.sourceColumns = JSON.stringify(operation.sourceColumns);
    operation.valueExpression = createCaseValueExpression(operation.conditionsMap);
    operation.withExpressions = "[]";
    return operation;
}

function createCaseValueExpression(conditionsMap) {
    let valueExpression = 'case ';
    conditionsMap?.conditions?.forEach(condition => {
        valueExpression += `when \`${condition.sourceColumn}\` ${condition.logicalOperator} ${condition.value} then '${condition.result}' `;
    })
    valueExpression += `else '${conditionsMap?.defaultresult}' end`;
    return valueExpression;
}

function createRecipe(recipeName, operationsArray) {
    return new CreateRecipeCommand({
        Name: recipeName,
        Steps: operationsArray.map(operation => ({
            Action: {
                Operation: operation.name,
                Parameters: createRecipeOperationParams(operation)
            }
        }))
    });
}

function createRecipeOperationParams(operation) {
    switch(operation.name) {
        case 'CASE_OPERATION':
            return {
                functionStepType : operation.functionStepType,
                targetColumn: operation.targetColumn,
                valueExpression: operation.valueExpression,
                withExpressions: operation.withExpressions
            };
        case 'MERGE':
            return {
                sourceColumns : operation.sourceColumns,
                delimiter: operation.delimiter,
                targetColumn: operation.targetColumn
            };
        case 'RENAME':
        case 'DUPLICATE':
            return {
                sourceColumn : operation.sourceColumn,
                targetColumn: operation.targetColumn
            };
        case 'FORMAT_DATE':
            return {
                sourceColumn : operation.sourceColumn,
                targetDateFormat: operation.targetDateFormat
            };
        case 'DELETE':
            return {
                sourceColumns : operation.sourceColumns
            };
        case 'SPLIT_COLUMN_SINGLE_DELIMITER':
            return {
                includeInSplit : operation.includeInSplit,
                limit: operation.limit,
                pattern: operation.pattern,
                sourceColumn: operation.sourceColumn
            };
        default:
            return {};
    }
}

function createDataset(datasetParams) {
    return new CreateDatasetCommand({
        Name: datasetParams.datasetName,
        Input: {
            DatabaseInputDefinition: {
                GlueConnectionName: datasetParams.glueConnectionName,
                DatabaseTableName: datasetParams.databaseTableName,
            }
        }
    });
}

function createProject(projectName, recipeName, datasetParams, roleArn) {
    return new CreateProjectCommand({
        Name: projectName,
        RecipeName: recipeName,
        DatasetName: datasetParams.datasetName,
        RoleArn: roleArn,
    });
}

function createDataBrewJob(jobParams, projectName, roleArn) {
    return new CreateRecipeJobCommand({
        Name: jobParams.jobName,
        ProjectName: projectName,
        RoleArn: roleArn,
        Outputs: [{
            Location: {
                Bucket: jobParams.s3OutputBucketName,
                Key: jobParams.s3OutputPath
            },
            Format: jobParams.outputFileFormat
        }]
    });
}

exports.brewTask = async (req, res) => {
//app.post('/create-and-run-brew', async (req, res) => {
    const { recipeName, datasetParams, projectName, jobParams, operationsArray } = req.body;
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

    try {
       
        const input_operation_array = operationsArray.map(createOperation);

        const createRecipeCommand = createRecipe(recipeName, input_operation_array);
        const recipeResponse = await databrewClient.send(createRecipeCommand);
        console.log(`Recipe created: ${recipeResponse.Name}`);

        const createDataSetCommand = createDataset(datasetParams);
        const datasetResponse = await databrewClient.send(createDataSetCommand);
        console.log('Dataset Created');

        const createProjectCommand = createProject(projectName, recipeName, datasetParams, awsCredentials.Role);
        const projectResponse = await databrewClient.send(createProjectCommand);
        console.log(`Project created: ${projectResponse.Name}`);

        const createRecipeJobCommand = createDataBrewJob(jobParams, projectName, awsCredentials.Role);
        const jobResponse = await databrewClient.send(createRecipeJobCommand);
        console.log(`Job created: ${jobResponse.Name}`);

        const startJobResponse = await databrewClient.send(new StartJobRunCommand({ Name: jobParams.jobName }));
        console.log('Job started:', startJobResponse);

        // // Periodically check job status
        // let intervalId = setInterval(async () => {
        //     const describeResponse = await databrewClient.send(new DescribeJobRunCommand({
        //         Name: jobParams.jobName,
        //         RunId: startJobResponse.RunId,
        //     }));
        //     console.log(`Job status: ${describeResponse.State}`);
        //     if (['SUCCEEDED', 'FAILED', 'STOPPED'].includes(describeResponse.State)) {
        //         clearInterval(intervalId);
        //         if (describeResponse.State === 'SUCCEEDED') {
        //             // Send success response
        //             res.status(200).json({
        //                 status: 'Success',
        //                 message: 'Data transformation job completed successfully.',
        //                 details: {
        //                     recipeName: recipeName,
        //                     jobName: jobParams.jobName,
        //                     outputLocation: jobResponse.Outputs[0].Location,
        //                     outputFileFormat: jobParams.outputFileFormat,
        //                     datasetUsed: datasetParams.datasetName
        //                 }
        //             });
        //         } else {
        //             // Send failure response
        //             res.status(500).json({
        //                 status: 'Failed',
        //                 message: `Job failed with state: ${describeResponse.State}`,
        //                 details: describeResponse.Error
        //             });
        //         }
        //     }
        // }, 5000); 

        let intervalId = setInterval(async () => {
            const describeResponse = await databrewClient.send(new DescribeJobRunCommand({
                Name: jobParams.jobName,
                RunId: startJobResponse.RunId,
            }));
            console.log(`Job status: ${describeResponse.State}`);
            if (['SUCCEEDED', 'FAILED', 'STOPPED'].includes(describeResponse.State)) {
                clearInterval(intervalId);
                if (describeResponse.State === 'SUCCEEDED') {
                   // if (jobResponse.Outputs && jobResponse.Outputs.length > 0) {
                        res.status(200).json({
                            status: 'Success',
                            message: 'Data transformation job completed successfully.',
                            details: {
                                recipeName: recipeName,
                                jobName: jobParams.jobName,
                                //outputLocation: jobResponse.Outputs[0].Location,
                                outputLocation: jobParams.s3OutputPath,
                                outputFileFormat: jobParams.outputFileFormat,
                                datasetUsed: datasetParams.datasetName,
                               // outputResponse: jobResponse.Outputs
                            }
                        });
                    // } else {
                    //     res.status(500).json({
                    //         status: 'Failed',
                    //         message: 'Job completed, but no output location defined.',
                    //     });
                    }
                 else {
                    // Send failure response
                    res.status(500).json({
                        status: 'Failed',
                        message: `Job failed with state: ${describeResponse.State}`,
                        details: describeResponse.Error
                    });
                }
            }
        }, 5000); 
        


    } catch (error) {
        console.error('Error:', error);
        res.status(500).send(`An error occurred: ${error.message}`);
    }
};

// Start the server
// const port = 3007;
// const server = app.listen(port, () => {
//     console.log(`Server is running on http://localhost:${port}`);
// });

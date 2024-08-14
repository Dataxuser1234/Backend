const express = require('express');
const {
    DataBrewClient,
    CreateRecipeCommand,
    CreateProjectCommand,
    CreateRecipeJobCommand,
    CreateDatasetCommand,
    ListRecipesCommand,
    ListDatasetsCommand,
    ListProjectsCommand,
    ListJobsCommand,
    StartJobRunCommand
} = require('@aws-sdk/client-databrew');
const dotenv = require('dotenv').config();
const awsCredentials = require('../resource/awsConfig.json');
const fs = require('fs');

const app = express();
app.use(express.json()); // Middleware to parse JSON

// Error handling for JSON parsing
app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
        console.error('Bad JSON error:', err);
        return res.status(400).send({ error: 'Invalid JSON format' });
    }
    next();
});

const databrewClient = new DataBrewClient({
    region: awsCredentials.region,
    credentials: awsCredentials,
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
    console.log('Creating operation for:', operation.name); // Log operation name
    if (operation.name === 'CASE_OPERATION') {
        return {
            Operation: 'CASE_OPERATION',
            Parameters: {
                TargetColumn: String(operation.targetColumn),
                ValueExpression: createCaseValueExpression(operation.conditionsMap),
                WithExpressions: JSON.stringify(operation.withExpressions || [])
            }
        };
    } else if (operation.name === 'MERGE') {
        return {
            Operation: 'MERGE',
            Parameters: {
                SourceColumns: operation.sourceColumns.join(','), // Ensure this is a comma-separated string
                Delimiter: String(operation.delimiter),
                TargetColumn: String(operation.targetColumn)
            }
        };
    } else if (operation.name === 'RENAME' || operation.name === 'DUPLICATE') {
        return {
            Operation: operation.name.toUpperCase(),
            Parameters: {
                SourceColumn: String(operation.sourceColumn),
                TargetColumn: String(operation.targetColumn)
            }
        };
    } else if (operation.name === 'FORMAT_DATE') {
        return {
            Operation: 'FORMAT_DATE',
            Parameters: {
                SourceColumn: String(operation.sourceColumn),
                DateFormat: String(operation.targetDateFormat)
            }
        };
    } else if (operation.name === 'DELETE') {
        return {
            Operation: 'DELETE',
            Parameters: {
                SourceColumns: operation.sourceColumns.join(',') // Ensure this is a comma-separated string
            }
        };
    } else if (operation.name === 'SPLIT_COLUMN_SINGLE_DELIMITER') {
        return {
            Operation: 'SPLIT_COLUMN',
            Parameters: {
                SourceColumn: String(operation.sourceColumn),
                Delimiter: String(operation.pattern),
                IncludeInSplit: String(operation.includeInSplit),
                Limit: String(operation.limit)
            }
        };
    } else {
        console.error('Unknown operation:', operation.name);
        return null;
    }
}

function createCaseValueExpression(conditionsMap) {
    let valueExpression = 'CASE ';
    conditionsMap.conditions.forEach(condition => {
        valueExpression += `WHEN \`${condition.sourceColumn}\` ${condition.logicalOperator} ${condition.value} THEN '${condition.result}' `;
    });
    valueExpression += `ELSE '${conditionsMap.defaultresult}' END`;
    return valueExpression;
}

app.post('/create-and-run-brew', async (req, res) => {
    const { recipeName, datasetParams, projectName, jobParams, operationsArray } = req.body;

    try {
        // Validate input parameters
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

        const input_operations = operationsArray.map(createOperation).filter(op => op !== null);

        const createRecipeCommand = new createRecipeCommand({
            Name: recipeName,
            Steps: input_operations.map(op => ({ Action: op }))
        });

        console.log('CreateRecipeCommand input:', JSON.stringify(createRecipeCommand.input, null, 2));
        const createRecipeResponse = await databrewClient.send(createRecipeCommand);

        const createDatasetCommand = new CreateDatasetCommand({
            Name: datasetParams.datasetName,
            Input: {
                DatabaseInputDefinition: {
                    GlueConnectionName: datasetParams.glueConnectionName,
                    DatabaseTableName: datasetParams.databaseTableName,
                }
            }
        });
        console.log('Sending CreateDatasetCommand:', JSON.stringify(createDatasetCommand, null, 2));
        const createDatasetResponse = await databrewClient.send(createDatasetCommand);

        const createProjectCommand = new CreateProjectCommand({
            Name: projectName,
            RecipeName: recipeName,
            DatasetName: datasetParams.datasetName,
            RoleArn: jobParams.roleArn,
        });
        console.log('Sending CreateProjectCommand:', JSON.stringify(createProjectCommand, null, 2));
        const createProjectResponse = await databrewClient.send(createProjectCommand);

        const createJobCommand = new CreateRecipeJobCommand({
            Name: jobParams.jobName,
            ProjectName: projectName,
            RoleArn: jobParams.roleArn,
            Outputs: [{
                Location: {
                    Bucket: jobParams.s3OutputBucketName,
                    Key: jobParams.s3OutputPath
                },
                Format: jobParams.outputFileFormat
            }]
        });
        console.log('Sending CreateRecipeJobCommand:', JSON.stringify(createJobCommand, null, 2));
        const createJobResponse = await databrewClient.send(createJobCommand);

        const startJobResponse = await databrewClient.send(new StartJobRunCommand({ Name: jobParams.jobName }));
        console.log('Start Job Response:', startJobResponse);

        res.status(200).json({
            message: 'All resources created successfully',
            recipeName: createRecipeResponse.Name,
            datasetName: createDatasetResponse.Name,
            projectName: createProjectResponse.Name,
            jobName: createJobResponse.Name,
            outputLocation: `${jobParams.s3OutputBucketName}/${jobParams.s3OutputPath}`
        });
    } catch (error) {
        console.error('Error processing request:', error);
        res.status(500).send({ error: 'An error occurred during processing', details: error.message });
    } finally {
       

        if (server != null) {
            server.close(() => {
                console.log('Server has been stopped');
            });
        }
    }
});

const server = app.listen(3006, () => {
    console.log('Server is running on port 3006');
});

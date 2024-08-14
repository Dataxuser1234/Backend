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
    ListJobsCommand
} = require('@aws-sdk/client-databrew');
const dotenv = require('dotenv').config();
const awsCredentials = require('../resource/awsConfig.json');

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
    if (operation.name == 'CASE_OPERATION') {
        return {
            Operation: 'CASE',
            Parameters: {
                TargetColumn: operation.targetColumn,
                ValueExpression: operation.valueExpression,
                WithExpressions: operation.withExpressions
            }
        };
    } else if (operation.name == 'MERGE') {
        return {
            Operation: 'MERGE',
            Parameters: {
                SourceColumns: operation.sourceColumns.join(','),
                Delimiter: operation.delimiter,
                TargetColumn: operation.targetColumn
            }
        };
    } else if (operation.name == 'RENAME' || operation.name == 'DUPLICATE') {
        return {
            Operation: operation.name.toUpperCase(),
            Parameters: {
                SourceColumn: operation.sourceColumn,
                TargetColumn: operation.targetColumn
            }
        };
    } else if (operation.name == 'FORMAT_DATE') {
        return {
            Operation: 'FORMAT_DATE',
            Parameters: {
                SourceColumn: operation.sourceColumn,
                DateFormat: operation.targetDateFormat
            }
        };
    } else if (operation.name == 'DELETE') {
        return {
            Operation: 'DELETE',
            Parameters: {
                SourceColumns: operation.sourceColumns.join(',')
            }
        };
    } else if (operation.name == 'SPLIT_COLUMN_SINGLE_DELIMITER') {
        return {
            Operation: 'SPLIT_COLUMN',
            Parameters: {
                SourceColumn: operation.sourceColumn,
                Delimiter: operation.pattern,
                IncludeInSplit: operation.includeInSplit.toString(),
                Limit: operation.limit.toString()
            }
        };
    }
}

app.post('/create-and-run-brew', async (req, res) => {
    const { recipeName, datasetParams, projectName, jobParams, operationsArray } = req.body;

    try {
        const resourcesExist = await Promise.all([
            checkResourceExists(recipeName, 'recipe'),
            checkResourceExists(datasetParams.datasetName, 'dataset'),
            checkResourceExists(projectName, 'project'),
            checkResourceExists(jobParams.jobName, 'job')
        ]);

        if (resourcesExist.some(exists => exists)) {
            return res.status(400).send({ error: 'One or more resources already exist' });
        }

        const input_operations = operationsArray.map(createOperation);

        const createRecipeCommand = new CreateRecipeCommand({
            Name: recipeName,
            Steps: input_operations.map(op => ({ Action: op }))
        });

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
        const createDatasetResponse = await databrewClient.send(createDatasetCommand);

        const createProjectCommand = new CreateProjectCommand({
            Name: projectName,
            RecipeName: recipeName,
            DatasetName: datasetParams.datasetName,
            RoleArn: jobParams.roleArn,
        });
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
        const createJobResponse = await databrewClient.send(createJobCommand);

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
    }
});

app.listen(3005, () => {
    console.log('Server is running on port 3005');
});

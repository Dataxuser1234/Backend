const express = require('express');
const multer = require('multer');
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const app = express();
const upload = multer({ dest: 'uploads/' });

app.post('/upload', upload.fields([{ name: 'dataFile' }, { name: 'idsFile' }]), (req, res) => {
    const dataFilePath = req.files['dataFile'][0].path;
    const idsFilePath = req.files['idsFile'][0].path;
    const idColumn = req.query.idColumn; // ID column name from query parameters

    // Read IDs from the second file into a set
    let ids = new Set();
    fs.createReadStream(idsFilePath)
        .pipe(csv())
        .on('data', (row) => {
            if (row[idColumn]) {
                const trimmedId = row[idColumn].trim();
                ids.add(trimmedId);
                console.log(`ID added: '${trimmedId}'`);
            } else {
                console.log(`Missing ID column in IDs file row: ${JSON.stringify(row)}`);
            }
        })
        .on('end', () => {
            console.log(`Total IDs loaded: ${ids.size}`);
            console.log("Starting data file processing...");

            // Now read the data file and filter by IDs
            let matchedData = [];
            fs.createReadStream(dataFilePath)
                .pipe(csv())
                .on('data', (row) => {
                    const rowId = row[idColumn] ? row[idColumn].trim() : undefined;
                    console.log(`Processing row ID: '${rowId}'`);
                    if (rowId && ids.has(rowId)) {
                        matchedData.push(row);
                        console.log(`Match found for ID: '${rowId}'`);
                    }
                })
                .on('end', () => {
                    console.log(`Total matches found: ${matchedData.length}`);
                    if (matchedData.length > 0) {
                        // Write matched data to a new CSV file
                        const csvWriter = createCsvWriter({
                            path: 'output.csv',
                            header: Object.keys(matchedData[0]).map(key => ({ id: key, title: key }))
                        });

                        csvWriter.writeRecords(matchedData)
                            .then(() => {
                                console.log('The CSV file was written successfully');
                                res.send('Matched data has been written to output.csv');
                            }).catch(err => {
                                console.error('Error writing to CSV:', err);
                                res.status(500).send('Error writing matched data to file');
                            });
                    } else {
                        res.send('No matching data found.');
                    }
                });
        });
});

const PORT = 3030;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});

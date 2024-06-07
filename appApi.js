const express = require('express');
const { multerMiddleware, uploadFile, authenticateJWT } = require('./Implementation/fileHandler');
const { getSchema,  } = require('./Implementation/schemaHandler');
const { startColumnStats, } = require('./Implementation/statsRunner');
const { getStats, } = require('./Implementation/statsInfoHandler');
const { getToken } = require('./Implementation/jsonwebtoken');
const { brewTask } = require('./Implementation/brewcopy');
const db = require('./login/queries.js');
const con = require('./login/const.js');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;
require('dotenv').config();

app.use(express.json());
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
);


app.get('/', (request, response) => {
    response.json({ info: 'Node.js, Express, and Postgres API' });
});

app.get('/api/login', db.login);
app.post('/api/signup', db.signup);
app.patch('/api/updateUser',db.updateUser)

app.post('/api/upload', authenticateJWT,multerMiddleware, uploadFile);
app.get('/api/getSchema', authenticateJWT,getSchema);
app.post('/api/startColumnStats', authenticateJWT,startColumnStats);
app.get('/api/getColumnStats',authenticateJWT, getStats);
app.get('/api/getToken', getToken);
app.post('/api/brewTask',authenticateJWT,brewTask)

app.listen(port, () => {
    console.log(` API server running on port ${port}`);
});

// app.listen(con.node_server, () => {
//   console.log(`App running on port ${con.node_server}.`);
// });
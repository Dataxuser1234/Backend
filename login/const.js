const DB_CONFIG = {
    user: 'Ram',
    password: 'Ram123',
    host: 'localhost',
    port: 5432,
    database: 'postgrestest',
};

const node_server = 3035
const table_name = 'user_login'
// Define the SQL queries


module.exports = {
    DB_CONFIG,
    node_server,
    table_name,
};

const { Client } = require('pg');
const { DB_CONFIG } = require('./const');
const { table_name } = require('./const'); 
const client = new Client(DB_CONFIG);

client.connect()
.then(() => {
    console.log('Connected to PostgreSQL database');
})
.catch((err) => {
    console.error('Error connecting to PostgreSQL database', err);
});

////fetch
exports.login = (email, password, callback) => {
    client.query(
        'SELECT EXISTS (SELECT 1 FROM ' + table_name + ' WHERE email = $1 AND password = $2)',
        [email, password],
        (error, results) => {
            if (error) {
                console.error('Error fetching user', error);
                return callback(error, false); 
            }

            const exists = results.rows[0].exists;
            callback(null, exists); // Return whether the user exists or not
        }
    );
};



////insert
exports.signup = (request, response) => {
    const { username, password, id, email } = request.body;
    client.query('INSERT INTO ' + table_name + ' (username, password, email) VALUES ($1, $2, $3)', [username, password, email], (error, results) => {
        if (error) {
            if (error.code === '23505' && error.constraint === 'unique_email') {
                return response.status(400).json({ status: 400, message: 'Email already exists' });
            }
            console.error('Error creating user', error);
            return response.status(500).json({ status: 500, message: 'Internal server error' });
        }
        
        return response.status(200).json({ status: 200, message: `User added with email ID: ${email}` }); 
        
    });
};

////patch
exports.updateUser = (request, response) => {
    const { emailid } = request.query;
    const { username, password, email } = request.body;

    console.log(emailid);

    const fields = [];
    const values = [];

    if (username) {
        fields.push('username');
        values.push(username);
    }

    if (password) {
        fields.push('password');
        values.push(password);
    }

    if (email) {
        fields.push('email');
        values.push(email);
    }

    if (fields.length === 0) {
        return response.status(400).json({ status: 400, message: 'No fields to update' });
    }

    const setQuery = fields.map((field, index) => `${field} = $${index + 1}`).join(', ');
    // console.log('******** PATCH CODE ********');
    // console.log('');
    // console.log('fields', fields);
    // console.log('values', values);
    // console.log('setQuery', setQuery);
    const query = `UPDATE ${table_name} SET ${setQuery} WHERE email = $${fields.length + 1}`;
    values.push(emailid);

    console.log(query);

    client.query(query, values, (error, results) => {
        if (error) {
            console.error('Error updating user', error);
            return response.status(500).json({ status: 500, message: 'Internal server error' });
        }

        if (results.rowCount === 0) {
            return response.status(404).json({ status: 404, message: 'User not found' });
        }

        return response.status(200).json({ status: 200, message: `User updated successfully` });
    });
};

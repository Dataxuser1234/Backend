const jwt = require('jsonwebtoken');
//const express = require('express');
//const app = express();
const { login } = require('../login/queries.js');

const secretKey = 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=';


exports.getToken = (req, res) => {
    const { email, password } = req.headers;
    console.log(email, password);

    login(email, password, (error, exists) => {
        if (error) {
            return res.status(500).json({ statusCode: 500, statusMessage: 'Internal server error', error: error.message });
        }

        if (!exists) {
            return res.status(401).json({
                statusCode: 401,
                statusMessage: 'Unauthorized',
                error: 'Invalid email or password'
            });
        }

        const token = jwt.sign({ email }, 'hDJwwy7Is1wO44qneiRjgTHbxA8Ie4mR75BvHJJ8qn8=', { expiresIn: '1h' });
        res.json({ token });
    });
};


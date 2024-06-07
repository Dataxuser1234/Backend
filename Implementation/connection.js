const mysql = require('mysql2');


// MySQL database connection details
const RDS_HOST = 'database-2.civ74iupife5.us-east-1.rds.amazonaws.com'; // rds endpoint
const RDS_USER = 'admin';
const RDS_PASSWORD = 'dataxform';
const RDS_DATABASE = 'database-2';

// Create MySQL connection
const con = mysql.createConnection({
  host: RDS_HOST,
  user: RDS_USER,
  password: RDS_PASSWORD,
  database: RDS_DATABASE
});

console.log(RDS_HOST)
console.log(RDS_USER)
console.log(RDS_PASSWORD)
console.log(RDS_DATABASE)
// Connect to the database
con.connect((err) => {
    console.log('Trying connect to database .............');
  if (err) {
    console.log('Error connecting to MySQL database: ', err);
   
    return;
  }
  console.log('Connected to MySQL database.');
});

// Perform database operations here...

// Close the connection when done
// con.end((err) => {
//   if (err) {
//     console.error('Error closing MySQL connection:', err);
//     return;
//   }
//   console.log('MySQL connection closed.');
// });

// mysql -h database-2.civ74iupife5.us-east-1.rds.amazonaws.com -P 3306 -u admin -p



// mysql -h database-2.civ74iupife5.us-east-1.rds.amazonaws.com -P 3306 -u admin -p --ssl

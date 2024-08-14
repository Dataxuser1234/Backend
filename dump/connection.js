const mysql = require('mysql2');


// MySQL database connection details
const RDS_HOST = 'database-2.civ74iupife5.us-east-1.rds.amazonaws.com'; // rds endpoint
const RDS_USER = 'admin';
const RDS_PASSWORD = 'dataxform';
const RDS_DATABASE = 'database3';

// Create MySQL connection
const con = mysql.createConnection({
  host: RDS_HOST,
  user: RDS_USER,
  password: RDS_PASSWORD,
  database: RDS_DATABASE
});


// Connect to the database
con.connect((err) => {
    console.log('Trying connect to database .............');
  if (err) {
    console.log('Error connecting to MySQL database: ', err);
   
    return;
  }
  console.log('Connected to MySQL database.');
  
  con.query('SHOW TABLES', (err, results, fields) => {
    if (err) {
      console.log('Error fetching tables: ', err);
    } else {
      console.log('Tables in the database:');
      results.forEach((row) => {
        console.log(Object.values(row)[0]); // Print the table name
      });
    }


    con.query('DESC People_Test_2M', (err, results) => {
    if (err) {
        console.log('Error executing query: ', err);
    } else {
        console.log('Table Columns : ', results);
    }
});


//WHERE `Index` != ""

//   con.query('DELETE FROM test12kdata_csv', (err, results, fields) => {
//     if (err) {
//         console.log('Error executing query: ', err);
//     } else {
//         console.log('Rows deleted: ', results.affectedRows);
//     }
// });


  con.query('SELECT COUNT(*) FROM People_Test_2M', (err, results) => {
    if (err) {
        console.log('Error executing query: ', err);
    } else {
        console.log('Count : ', results);
    }
});


con.end((err) => {
  if (err) {
    console.log('Error closing the connection: ', err);
  } else {
    console.log('Database connection closed.');
  }
});
// Org_Test_Data_10K
// Organisation_10K_data
// Organisation_new_10K_data
// my_table
// my_table2
// my_table4
// organizations_500000
// test12kdata_csv 
  

});
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
//  dataxform



// mysql -h database-2.civ74iupife5.us-east-1.rds.amazonaws.com -P 3306 -u admin -p --ssl






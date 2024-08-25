from flask import Blueprint, request, jsonify
import pyodbc
import logging
import hashlib  # For password hashing

# Create a Blueprint for user authentication
signup_bp = Blueprint('auth', __name__)

# Database connection setup
server = 'scrapedtreatmentsdatabase.database.windows.net'
database = 'scrapedtreatmentssqldatabase'
username = 'mzandi'
password = 'Ranger22!'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_users_table_if_not_exists():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if the Users table exists
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Users' AND xtype='U')
        CREATE TABLE Users (
            UserID INT PRIMARY KEY IDENTITY(1,1),
            Username NVARCHAR(255) NOT NULL,
            Email NVARCHAR(255) NOT NULL,
            Password NVARCHAR(255) NOT NULL,
            Role NVARCHAR(50) NOT NULL
        );
        """)
        conn.commit()
    except Exception as e:
        logging.error(f"Error checking or creating Users table: {e}")
    finally:
        cursor.close()
        conn.close()

@signup_bp.route('/signup', methods=['POST'])
def signup():
    # Ensure the Users table exists
    create_users_table_if_not_exists()

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        data = request.json
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        role = data.get('role')

        # Debug logging
        logging.info(f"Received signup data: {data}")

        # Check if the username or email already exists
        cursor.execute("SELECT COUNT(*) FROM Users WHERE Username = ? OR Email = ?", (username, email))
        count = cursor.fetchone()[0]

        if count > 0:
            return jsonify({"status": "error", "message": "Username or email already exists."}), 400

        # Hash the password before storing it
        hashed_password = hash_password(password)

        # Insert the new user into the database and retrieve UserID using OUTPUT clause
        query = """
        INSERT INTO Users (Username, Email, Password, Role)
        OUTPUT INSERTED.UserID
        VALUES (?, ?, ?, ?);
        """
        
        # Log before execution
        logging.info("Inserting user into the database")
        cursor.execute(query, (username, email, hashed_password, role))
        user_id = cursor.fetchval()  # Get the last inserted UserID
        
        # Ensure the transaction is committed
        conn.commit()

        # Log after successful insertion
        logging.info(f"User inserted with UserID: {user_id}")

        return jsonify({"status": "success", "message": "User signed up successfully.", "userID": user_id}), 201
    except Exception as e:
        logging.error(f"Error during sign-up: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

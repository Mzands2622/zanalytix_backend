import pyodbc
import os
import secrets
from flask import Blueprint, jsonify, request

forgot_password_bp = Blueprint('forgot_password', __name__)

# Database connection settings
server = 'scrapedtreatmentsdatabase.database.windows.net'
database = 'scrapedtreatmentssqldatabase'
username = 'mzandi'
password = 'Ranger22!'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

def generate_reset_token(email):
    token = secrets.token_urlsafe(32)
    return token

@forgot_password_bp.route('/forgot-password', methods=['POST'])
def forgot_password():
    data = request.get_json()
    email = data.get('email')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if the email exists in the database
        cursor.execute("SELECT UserID FROM Users WHERE Email = ?", email)
        user = cursor.fetchone()

        if not user:
            return jsonify({"message": "User with this email does not exist."}), 400

        # Generate reset token and print it to the console
        reset_token = generate_reset_token(email)
        print(f"Generated reset token for {email}: {reset_token}")

        # Instead of storing the token, we just return a success message
        return jsonify({"message": "A reset link has been generated and logged to the console."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()

@forgot_password_bp.route('/reset-password/<token>', methods=['POST'])
def reset_password(token):
    data = request.get_json()
    new_password = data.get('password')

    # Simulate the reset process without database interaction
    print(f"Received reset request with token: {token}")
    print(f"New password: {new_password} (In a real application, this would be hashed and stored.)")

    return jsonify({"message": "Password reset process simulated. Check console for details."}), 200
from flask import Blueprint, jsonify, request
import pyodbc


# Define the blueprint
preferences_bp = Blueprint('data_api', __name__)

# SQL Server connection setup
server = 'scrapedtreatmentsdatabase.database.windows.net'
database = 'scrapedtreatmentssqldatabase'
username = 'mzandi'
password = 'Ranger22!'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Function to get SQL connection
def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

# Endpoint to fetch categories
@preferences_bp.route('/api/categories', methods=['GET'])
def get_categories():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT category FROM Categories")  # Adjust SQL as needed
    rows = cursor.fetchall()
    categories = [row.category for row in rows]
    cursor.close()
    conn.close()
    return jsonify(categories)

# Endpoint to fetch companies based on categories
@preferences_bp.route('/api/companies', methods=['GET'])
def get_companies_by_category():
    category = request.args.get('category', '')
    
    if not category:
        return jsonify([])

    conn = get_db_connection()
    cursor = conn.cursor()

    # Build the SQL query to search for companies with the selected category
    query = "SELECT Company_Name FROM Profile_Table WHERE Categories LIKE '%' + ? + '%'"

    cursor.execute(query, [category])
    rows = cursor.fetchall()
    
    companies = [row.Company_Name for row in rows]
    
    cursor.close()
    conn.close()
    
    return jsonify(companies)

@preferences_bp.route('/api/all-companies', methods=['GET'])
def get_all_companies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Company_Name FROM Profile_Table")  # Adjust SQL as needed
    rows = cursor.fetchall()
    companies = [row.Company_Name for row in rows]
    cursor.close()
    conn.close()
    return jsonify(companies)


@preferences_bp.route('/save-or-update-notification-preferences', methods=['POST'])
def save_or_update_notification_preferences():
    try:
        data = request.json
        print("Received data:", data)
        conn = get_db_connection()
        cursor = conn.cursor()

        create_notification_request_object_table(conn)

        user_id = data.get('UserID')

        # Check if the data is a single preference set or an array of preference sets
        if isinstance(data, dict) and 'preferenceSets' not in data:
            preference_sets = [data]  # Treat single set as a list with one item
        else:
            preference_sets = data.get('preferenceSets', [])
        
        print(f"Processing {len(preference_sets)} preference sets for UserID: {user_id}")

        # Fetch existing columns from the database table
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Notification_Request_Object_Table'")
        existing_columns = [row[0] for row in cursor.fetchall()]

        for index, preference_set in enumerate(preference_sets):
            print(f"Processing preference set {index + 1}:")
            set_id = preference_set.get('SetID')
            
            # Generate SetTitle based on the current count + index
            set_title = preference_set.get('SetTitle', "")
            notification_priority = preference_set.get('Priority', 3)
            if notification_priority == True:
                notification_priority = 1
            first_name = preference_set.get('FirstName', '')
            last_name = preference_set.get('LastName', '')

            # Start building the columns and values
            columns = ['UserID', 'SetTitle', 'FirstName', 'LastName', 'Priority']
            values = [user_id, set_title, first_name, last_name, notification_priority]

            # Process categories
            for category, value in preference_set.get('categories', {}).items():
                if category in existing_columns:
                    columns.append(f"[{category}]")
                    values.append(1 if value else 0)
            
            # Process companies
            for company, value in preference_set.get('companies', {}).items():
                if company in existing_columns:
                    columns.append(f"[{company}]")
                    values.append(1 if value else 0)
            
            # Process info types
            for info_type, value in preference_set.get('infoTypes', {}).items():
                if info_type in existing_columns:
                    columns.append(f"[{info_type}]")
                    values.append(1 if value else 0)

            # Process pipelineDetails
            for detail, value in preference_set.get('pipelineDetails', {}).items():
                if detail in existing_columns:
                    columns.append(f"[{detail}]")
                    values.append(1 if value else 0)

            # Process financialDetails
            for detail, value in preference_set.get('financialDetails', {}).items():
                if detail in existing_columns:
                    columns.append(f"[{detail}]")
                    values.append(1 if value else 0)

            # Process personnelDetails
            for detail, value in preference_set.get('personnelDetails', {}).items():
                if detail in existing_columns:
                    columns.append(f"[{detail}]")
                    values.append(1 if value else 0)

            # Process preferred contacts
            for contact in preference_set.get('preferredContacts', []):
                contact_type = contact.get('contactType')
                contact_detail = contact.get('contactDetail')
                if contact_type in existing_columns:
                    columns.append(f"[{contact_type}]")
                    values.append(contact_detail)

            # Filter out invalid columns
            valid_columns_and_values = [(col, val) for col, val in zip(columns, values) if col.strip('[]') in existing_columns]

            # Unpack valid columns and values
            valid_columns = [cv[0] for cv in valid_columns_and_values]
            valid_values = [cv[1] for cv in valid_columns_and_values]

            print(f"Valid Columns: {valid_columns}")
            print(f"Valid Values: {valid_values}")

            if set_id is None:
                insert_query = f"""
                INSERT INTO Notification_Request_Object_Table ({', '.join(valid_columns)})
                OUTPUT INSERTED.SetID
                VALUES ({', '.join(['?' for _ in valid_columns])});
                """
                print(f"Executing INSERT query: {insert_query}")
                try:
                    cursor.execute(insert_query, valid_values)
                    result = cursor.fetchone()
                    if result:
                        set_id = result[0]
                        print(f"New SetID: {set_id}")
                    else:
                        print("Failed to retrieve new SetID")
                    conn.commit()
                    
                    rows_affected = cursor.rowcount
                    print(f"Rows affected by INSERT: {rows_affected}")
                    
                    if rows_affected == 0:
                        print("Warning: No rows were inserted")
                except pyodbc.Error as e:
                    conn.rollback()
                    print(f"Database error: {str(e)}")
                    raise
            else:
                update_assignments = ', '.join([f'{col} = ?' for col in valid_columns if col != 'UserID'])
                update_query = f"""
                UPDATE Notification_Request_Object_Table
                SET {update_assignments}
                WHERE SetID = ? AND UserID = ?
                """
                print(f"Executing UPDATE query: {update_query}")
                update_values = [v for k, v in zip(valid_columns, valid_values) if k != 'UserID'] + [set_id, user_id]
                cursor.execute(update_query, update_values)
                conn.commit()
                
                rows_affected = cursor.rowcount
                print(f"Rows affected by UPDATE: {rows_affected}")
                
                if rows_affected == 0:
                    print("Warning: No rows were updated")

        cursor.close()
        conn.close()

        return jsonify({"message": "Notification preferences saved or updated successfully"}), 200
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500


def create_notification_request_object_table(conn):
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Notification_Request_Object_Table')
        BEGIN
            CREATE TABLE Notification_Request_Object_Table (
                SetID INT PRIMARY KEY IDENTITY(1,1),
                UserID INT,
                SetTitle NVARCHAR(255),
                Priority INT,
                FirstName NVARCHAR(100),
                LastName NVARCHAR(100)
            )
        END
    """)
    conn.commit()

    # Fetch company names from Profile_Table
    cursor.execute("SELECT DISTINCT Company_Name FROM Profile_Table")
    companies = [row.Company_Name for row in cursor.fetchall()]

    # Fetch category names from Categories table
    cursor.execute("SELECT DISTINCT category FROM Categories")
    categories = [row.category for row in cursor.fetchall()]

    # Define contact types
    contact_types = ['email', 'text', 'call', 'instagram', 'facebook']

    info_types = ["Pipeline Info", "Financial Info", "Personell Info", "MAndA", "Layoffs", "NewHires", "TherapyApproval", "IndicationChange", "EarningsReport"]

    # Add missing columns dynamically if they don't exist
    for company in companies:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{company}' AND Object_ID = Object_ID(N'Notification_Request_Object_Table'))
            BEGIN
                ALTER TABLE Notification_Request_Object_Table ADD [{company}] BIT
            END
        """)
    for category in categories:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{category}' AND Object_ID = Object_ID(N'Notification_Request_Object_Table'))
            BEGIN
                ALTER TABLE Notification_Request_Object_Table ADD [{category}] BIT
            END
        """)
    for contact in contact_types:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{contact}' AND Object_ID = Object_ID(N'Notification_Request_Object_Table'))
            BEGIN
                ALTER TABLE Notification_Request_Object_Table ADD [{contact}] NVARCHAR(255)
            END
        """)

    for type in info_types:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{type}' AND Object_ID = Object_ID(N'Notification_Request_Object_Table'))
            BEGIN
                ALTER TABLE Notification_Request_Object_Table ADD [{type}] BIT
            END
        """)
    
    conn.commit()
    cursor.close()

@preferences_bp.route('/delete-notification-preference/<set_id>', methods=['DELETE'])
def delete_notification_preference(set_id):
    try:
        if set_id == 'null':
            return jsonify({"message": "New preference set removed"}), 200

        conn = get_db_connection()
        cursor = conn.cursor()

        delete_query = "DELETE FROM Notification_Request_Object_Table WHERE SetID = ?"
        cursor.execute(delete_query, (int(set_id),))

        if cursor.rowcount == 0:
            conn.close()
            return jsonify({"error": "Preference set not found"}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Preference set deleted successfully"}), 200
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500
import pyodbc
import json
from flask import Flask, request, jsonify
from flask_cors import CORS



app = Flask(__name__)
CORS(app)

@app.route('/add-scraping-object', methods=['POST'])
def add_scraping_object():
    conn = None
    cursor = None
    try:
        data = request.json
        if not data:
            return jsonify({'status': 'error', 'message': 'No data received'}), 400

        # Validate necessary keys in the JSON payload
        required_keys = ['companyId', 'companyName', 'headquartersLocalTime', 'scrapingObject']
        missing_keys = [key for key in required_keys if key not in data]
        if missing_keys:
            return jsonify({'status': 'error', 'message': f'Missing data for keys: {missing_keys}'}), 400

        print("Received data:", data)

        # Database connection setup
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 17 for SQL Server}'
        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Check if the profile exists
        cursor.execute("SELECT Scraping_Objects FROM Profile_Table WHERE Company_ID = ?", (data['companyId'],))
        result = cursor.fetchone()

        if result:
            # Update the existing record
            current_objects = json.loads(result[0]) if result[0] else []
            current_objects.append(data['scrapingObject'])  # Append only the scrapingObject
            updated_objects = json.dumps(current_objects)
            cursor.execute("""
                UPDATE Profile_Table
                SET 
                    Scraping_Objects = ?, 
                    Company_Name = ?, 
                    Headquarters_Local_Time = ? 
                WHERE Company_ID = ?""", 
                (updated_objects, data['companyName'], data['headquartersLocalTime'], data['companyId']))
        else:
            # Insert a new record if it does not exist
            new_objects = json.dumps([data['scrapingObject']])
            cursor.execute("""
                INSERT INTO Profile_Table (Company_ID, Company_Name, Headquarters_Local_Time, Scraping_Objects) 
                VALUES (?, ?, ?, ?)""", 
                (data['companyId'], data['companyName'], data['headquartersLocalTime'], new_objects))

        conn.commit()
        return jsonify({'status': 'success', 'data': data}), 200

    except pyodbc.Error as e:
        print("Database error:", str(e))
        return jsonify({'status': 'error', 'message': 'Database error: ' + str(e)}), 500
    except Exception as e:
        print("Error during processing:", str(e))
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/delete-scraping-object/<company_id>/<object_code>', methods=['DELETE'])
def delete_scraping_object(company_id, object_code):
    conn = None
    cursor = None
    try:
        # Database connection setup
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 17 for SQL Server}'
        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Fetch the current scraping objects from the profile
        cursor.execute("SELECT Scraping_Objects FROM Profile_Table WHERE Company_ID = ?", (company_id,))
        result = cursor.fetchone()
        if result:
            # Filter out the object to be deleted
            current_objects = json.loads(result[0])
            updated_objects = [obj for obj in current_objects if obj['objectCode'] != object_code]
            updated_json = json.dumps(updated_objects)

            # Update the database with the new list
            cursor.execute("UPDATE Profile_Table SET Scraping_Objects = ? WHERE Company_ID = ?", (updated_json, company_id))
            conn.commit()
            return jsonify({'status': 'success', 'message': 'Object deleted successfully'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Company not found'}), 404

    except pyodbc.Error as e:
        print("Database error:", str(e))
        return jsonify({'status': 'error', 'message': 'Database error: ' + str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/update-scraping-object/<company_id>/<object_code>', methods=['POST'])
def update_scraping_object(company_id, object_code):
    try:
        data = request.json
        scraping_object = data.get('scrapingObject', {})
        new_description = scraping_object.get('objectDescription')
        new_frequency = scraping_object.get('objectFrequency')

        # Print the received data to check accuracy
        print("Company ID:", company_id)
        print("Object Code:", object_code)
        print("New Description:", new_description)
        print("New Frequency:", new_frequency)

        # Setup database connection
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 17 for SQL Server}'
        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Get the current Scraping_Objects JSON from the database
        cursor.execute("SELECT Scraping_Objects FROM Profile_Table WHERE Company_ID = ?", (company_id,))
        result = cursor.fetchone()
        if result:
            current_objects = json.loads(result[0])

            # Find the object with the matching objectCode
            for obj in current_objects:
                if obj['objectCode'] == object_code:
                    obj['objectDescription'] = new_description
                    obj['objectFrequency'] = new_frequency
                    break

            # Convert the updated list back to JSON
            updated_objects_json = json.dumps(current_objects)

            # Update the database with the modified JSON
            cursor.execute("""
                UPDATE Profile_Table
                SET Scraping_Objects = ?
                WHERE Company_ID = ?
            """, (updated_objects_json, company_id))
            conn.commit()
            return jsonify({'status': 'success', 'message': 'Scraping object updated successfully'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'No such company found'}), 404

    except pyodbc.Error as e:
        print("Database error:", str(e))
        return jsonify({'status': 'error', 'message': str(e)}), 500
    except Exception as e:
        print("Error during processing:", str(e))
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



if __name__ == '__main__':
    app.run(debug=True, port=5000)
import pyodbc
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from dateutil.rrule import rrulestr, rrule
from datetime import datetime



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
        scraping_object = request.json['scrapingObject']
        add_to_calendar(scraping_object)
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

        # Step 1: Remove the scraping object from the Profile_Table
        cursor.execute("SELECT Scraping_Objects FROM Profile_Table WHERE Company_ID = ?", (company_id,))
        result = cursor.fetchone()
        if result:
            # Filter out the object to be deleted
            current_objects = json.loads(result[0])
            updated_objects = [obj for obj in current_objects if obj['objectCode'] != object_code]
            updated_json = json.dumps(updated_objects)

            # Update the Profile_Table with the new list
            cursor.execute("UPDATE Profile_Table SET Scraping_Objects = ? WHERE Company_ID = ?", (updated_json, company_id))
        
        # Step 2: Remove the scraping object from the Calendar table
        cursor.execute("SELECT Time, Scraping_Objects FROM Calendar")
        calendar_rows = cursor.fetchall()

        for row in calendar_rows:
            time, scraping_objects_json = row
            scraping_objects = json.loads(scraping_objects_json)

            # Filter out the object to be deleted from the current scraping objects
            updated_scraping_objects = [obj for obj in scraping_objects if obj['objectCode'] != object_code]

            if len(updated_scraping_objects) != len(scraping_objects):
                if updated_scraping_objects:
                    # Update the Calendar table with the updated scraping objects
                    updated_scraping_objects_json = json.dumps(updated_scraping_objects)
                    cursor.execute("""
                        UPDATE Calendar 
                        SET Scraping_Objects = ?
                        WHERE Time = ?
                    """, (updated_scraping_objects_json, time))
                else:
                    # If no scraping objects remain for that time, delete the row
                    cursor.execute("""
                        DELETE FROM Calendar 
                        WHERE Time = ?
                    """, (time,))

        conn.commit()
        return jsonify({'status': 'success', 'message': 'Object deleted successfully'}), 200

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
            scraping_object = request.json['scrapingObject']
            add_to_calendar(scraping_object)
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

def add_to_calendar(scraping_object):
    print(scraping_object)
    try:
        rrule_string = scraping_object['objectFrequency']
        rule = rrulestr(rrule_string)
        
        dates = []
        for date in rule:
            if date.year < 9999:  # Ensure date is within a valid range
                dates.append(date)
            if len(dates) >= 30:
                break
        
        print(dates)

        # Database connection setup
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 17 for SQL Server}'
        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # First, remove the object from all dates where it currently exists but shouldn't anymore
        cursor.execute("SELECT Time, Scraping_Objects FROM Calendar")
        rows = cursor.fetchall()

        for row in rows:
            time = row[0]
            current_objects = json.loads(row[1])

            # Find the object and check if it still belongs on this date
            updated_objects = []
            for obj in current_objects:
                if obj['objectCode'] == scraping_object['objectCode']:
                    if time not in dates:  # If the time is not in the new dates, remove it
                        continue
                updated_objects.append(obj)

            # If the list was modified, update the database
            if len(updated_objects) != len(current_objects):
                if updated_objects:
                    cursor.execute("UPDATE Calendar SET Scraping_Objects = ? WHERE Time = ?", 
                                   (json.dumps(updated_objects), time))
                else:
                    cursor.execute("DELETE FROM Calendar WHERE Time = ?", (time,))

        # Then, reinsert the object according to the new schedule
        for date in dates:
            # Adjust the time based on frequency directly in the loop
            if 'HOURLY' in rrule_string:
                date = date.replace(minute=0, second=0, microsecond=0)
            elif 'MINUTELY' in rrule_string:
                date = date.replace(second=0, microsecond=0)
            elif 'DAILY' in rrule_string or 'WEEKLY' in rrule_string or 'MONTHLY' in rrule_string or 'YEARLY' in rrule_string:
                date = date.replace(hour=0, minute=0, second=0, microsecond=0)

            date_str = date.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("SELECT Scraping_Objects FROM Calendar WHERE Time = ?", (date_str,))
            result = cursor.fetchone()

            if result:
                current_objects = json.loads(result[0])
                updated_objects = [obj if obj['objectCode'] != scraping_object['objectCode'] else scraping_object for obj in current_objects]
                if scraping_object not in updated_objects:
                    updated_objects.append(scraping_object)
                cursor.execute("UPDATE Calendar SET Scraping_Objects = ? WHERE Time = ?", 
                               (json.dumps(updated_objects), date_str))
            else:
                new_objects = json.dumps([scraping_object])
                cursor.execute("INSERT INTO Calendar (Time, Scraping_Objects) VALUES (?, ?)", 
                               (date_str, new_objects))

        conn.commit()

    except Exception as e:
        print("Error in add_to_calendar:", str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':
    app.run(debug=True, port=5000)
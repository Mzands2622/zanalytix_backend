import pyodbc
from flask import Blueprint, jsonify
from fetch_preference_options import create_notification_request_object_table

retreive_options_bp = Blueprint('retreive_options', __name__)

server = 'scrapedtreatmentsdatabase.database.windows.net'
database = 'scrapedtreatmentssqldatabase'
username = 'mzandi'
password = 'Ranger22!'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

@retreive_options_bp.route('/preferences/<user_id>', methods=['GET'])
def get_user_preferences(user_id):
    conn = get_db_connection()
    create_notification_request_object_table(conn)
    cursor = conn.cursor()

    try:
        # Fetch contact information from ClientContacts table
        cursor.execute("""
            SELECT FirstName, LastName, email, text, call, instagram, facebook
            FROM ClientContacts 
            WHERE UserID = ?
        """, (user_id,))
        contact_info = cursor.fetchone()

        if not contact_info:
            return jsonify({"error": "No contact information found for this user."}), 404

        # Prepare all contacts (both preferred and additional)
        all_contacts = []
        for contact_type in ['email', 'text', 'call', 'instagram', 'facebook']:
            if getattr(contact_info, contact_type):
                all_contacts.append({
                    "contactType": contact_type,
                    "contactDetail": getattr(contact_info, contact_type),
                    "preferred": False  # Will be set to True for preferred contacts later
                })

        # Fetch all preference sets for the user from Notification_Request_Object_Table
        cursor.execute(f"SELECT * FROM Notification_Request_Object_Table WHERE UserID = ?", (user_id,))
        all_user_data = cursor.fetchall()

        preference_sets = []

        for user_data in all_user_data:
            preferences_dict = {}
            preferred_contacts = []

            for column_name, value in zip(cursor.description, user_data):
                column_key = column_name[0]
                
                if value in (True, 1):
                    preferences_dict[column_key] = True
                elif column_key in ['UserID', 'SetID', 'SetTitle', 'Priority']:
                    preferences_dict[column_key] = value
                elif column_key in ['email', 'text', 'call', 'instagram', 'facebook'] and value:
                    preferred_contacts.append({
                        "contactType": column_key,
                        "contactDetail": value,
                        "preferred": True
                    })

            # Mark preferred contacts in all_contacts
            for contact in all_contacts:
                if any(pc["contactType"] == contact["contactType"] and pc["contactDetail"] == contact["contactDetail"] for pc in preferred_contacts):
                    contact["preferred"] = True

            preferences_dict['preferredContacts'] = all_contacts
            preference_sets.append(preferences_dict)

        # Combine the contact information and all preference sets
        full_preferences = {
            "contactInfo": {
                "firstName": contact_info.FirstName,
                "lastName": contact_info.LastName,
            },
            "preferenceSets": preference_sets
        }

        return jsonify(full_preferences), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()

@retreive_options_bp.route('/api/user-contact-information/<int:user_id>', methods=['GET'])
def get_user_contact_information(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
        SELECT TOP 1 FirstName, LastName, email, text, call, instagram, facebook
        FROM ClientContacts
        WHERE UserID = ?
        """
        cursor.execute(query, (user_id,))
        row = cursor.fetchone()

        if row:
            contact_info = {
                'firstName': row.FirstName,
                'lastName': row.LastName,
                'email': row.email,
                'text': row.text,
                'call': row.call,
                'instagram': row.instagram,
                'facebook': row.facebook
            }
            # Only include non-null and non-empty string values
            contact_info = {k: v for k, v in contact_info.items() if v and v.strip()}
        else:
            contact_info = None

        cursor.close()
        conn.close()

        print("contact_info", contact_info)
        return jsonify(contact_info), 200
    except Exception as e:
        print(f"Error fetching user contact information: {str(e)}")
        return jsonify({"error": str(e)}), 500
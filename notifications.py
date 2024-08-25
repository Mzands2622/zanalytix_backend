from flask import Blueprint, jsonify
import pyodbc
import json
import logging
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
import openai
import re
import smtplib
from twilio.rest import Client  # For SMS and call notifications

# Define a Blueprint
notifications_bp = Blueprint('notifications', __name__)

@notifications_bp.route('/trigger-notifications', methods=['GET'])
def trigger_notifications():
    conn = None
    cursor = None
    try:
        # Initialize OpenAI API and LLM
        openai.api_key = "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"
        llm = ChatOpenAI(temperature=0, model="gpt-4o")
        
        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["old_object", "new_object"],
            template="""
                Compare the following two JSON objects and provide a priority of change on a scale of 1-5 (1 being not important and 5 being extremely important).
                Also, provide a description of the change in a concise manner. DO NOT mention the changes involved with the Date_Scraped or the App_Notification fields at all.

                Based on the change, update the following template:

                {{
                    "Pipeline Info": false,
                    "Financial Info": false,
                    "Personell Info": false,
                    "TherapyApproval": false,
                    "IndicationChange": false,
                    "EarningsReport": false,
                    "MAndA": false,
                    "Layoffs": false,
                    "NewHires": false,
                    "priority": ,  // This field should reflect the actual priority based on the change (on a scale of 1-5)
                    "description": "",  // This field should be filled with the description of the change. Make sure to include the treatment name found in the objects.
                }}

                Old Object:
                {old_object}

                New Object:
                {new_object}

                Based on the comparison, update the fields in the JSON template to reflect the changes. Set the relevant fields to true if they are affected by the change.
                Please return only the updated JSON template with all of the key-value pairs originally given and nothing else. So that means do not respond with any summary. Only the JSON object has to be returned.
            """
        )

        # Set up the LLM chain
        llm_chain = prompt_template | llm

        # Database connection setup
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 17 for SQL Server}'

        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        create_stream_table(conn)

        # Fetch all treatment data
        cursor.execute("SELECT Treatment_Key, Treatment_Data FROM Revised_MasterTable")
        all_data = cursor.fetchall()

        # Convert data to dictionaries for easier comparison
        data_dict = {row[0]: json.loads(row[1]) for row in all_data}

        # Compare records and update notifications
        # Iterate over all treatment data
        for treatment_key, treatment_list in data_dict.items():
            # Assuming the last entry is the latest
            latest_record = treatment_list[-1]
            latest_date, latest_details = list(latest_record.items())[0]

            # Compare with previous records if available
            if len(treatment_list) > 1:
                previous_record = treatment_list[-2]
                previous_date, previous_details = list(previous_record.items())[0]

                # Convert objects to JSON strings for the prompt
                old_object_json = json.dumps(previous_details, indent=2)
                new_object_json = json.dumps(latest_details, indent=2)

                # Run the LLM chain with the old and new objects
                response = llm_chain.invoke({
                    "old_object": old_object_json,
                    "new_object": new_object_json
                })

                # Extract the JSON content from the response
                json_content = extract_json_from_response(response.content)

                try:
                    # Ensure json_content is a string before loading
                    if isinstance(json_content, dict):
                        json_content = json.dumps(json_content)

                    # Attempt to parse the cleaned JSON content
                    change_info = json.loads(json_content)

                    # Insert stream data with dictionaries
                    insert_stream_data(conn, json_content, previous_details, latest_details)

                    # Add the notification description to the App_Notification
                    latest_details['App_Notification'] = json.dumps([{"en": json_content}], ensure_ascii=False)

                except json.JSONDecodeError:
                    print("JSON failure:", json_content)
                    # Insert stream data with raw response
                    insert_stream_data(conn, json_content, previous_details, latest_details)
                    latest_details['App_Notification'] = json_content

            else:
                latest_details['App_Notification'] = json.dumps([{"en": "No previous record for comparison."}], ensure_ascii=False)

            # Update the treatment data with the new notification
            updated_json = json.dumps(treatment_list, ensure_ascii=False)
            update_query = "UPDATE Revised_MasterTable SET Treatment_Data = ? WHERE Treatment_Key = ?"
            cursor.execute(update_query, (updated_json, treatment_key))

        conn.commit()

        return jsonify({"status": "success", "message": "Notifications compared and updated successfully."}), 200

    except Exception as e:
        logging.error(f"An error occurred during comparison and update: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_stream_table(conn):
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Stream')
        BEGIN
            CREATE TABLE Stream (
                id INT PRIMARY KEY IDENTITY(1,1),
                priority INT,
                description NVARCHAR(255),
                timestamp DATETIME,
                raw_response NVARCHAR(MAX),
                old_object NVARCHAR(MAX),
                new_object NVARCHAR(MAX)
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

    info_types = ["Pipeline Info", "Financial Info", "Personell Info", "MAndA", "Layoffs", "NewHires", "TherapyApproval", "IndicationChange", "EarningsReport"]

    # Add company columns dynamically if they don't exist
    for company in companies:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{company}' AND Object_ID = Object_ID(N'Stream'))
            BEGIN
                ALTER TABLE Stream ADD [{company}] BIT
            END
        """)

    # Add category columns dynamically if they don't exist
    for category in categories:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{category}' AND Object_ID = Object_ID(N'Stream'))
            BEGIN
                ALTER TABLE Stream ADD [{category}] BIT
            END
        """)

    # Add info type columns dynamically if they don't exist
    for info_type in info_types:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'{info_type}' AND Object_ID = Object_ID(N'Stream'))
            BEGIN
                ALTER TABLE Stream ADD [{info_type}] BIT
            END
        """)

    conn.commit()
    cursor.close()

def extract_json_from_response(response):
    try:
        # Use regex to find the content between the first '{' and the last '}'
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            # Now try to parse the extracted string as JSON
            json_data = json.loads(json_str)
            print(json_data)
            print("----------")
            return json_data
        else:
            raise ValueError("No JSON object found in the response.")
    except (ValueError, json.JSONDecodeError) as e:
        # Handle cases where JSON parsing fails
        print(f"Error extracting JSON: {e}")
        return None


def insert_stream_data(conn, response, old_object, new_object):
    cursor = conn.cursor()

    try:
        # If response is already a dictionary, skip json.loads
        if isinstance(response, dict):
            data = response
        else:
            # Attempt to parse the response as JSON
            data = json.loads(response)

        # Handle default values
        priority = data.get("priority", 1)  # Default to 1 if priority is missing
        description = data.get("description", "No changes detected")  # Default to "No changes detected" if description is missing
        timestamp = data.get("timestamp", None)

        # Fetch company names from Profile_Table
        cursor.execute("SELECT DISTINCT Company_Name FROM Profile_Table")
        companies = [row.Company_Name for row in cursor.fetchall()]

        # Fetch category names from Categories table
        cursor.execute("SELECT DISTINCT category FROM Categories")
        categories = [row.category for row in cursor.fetchall()]

        # Define the info types
        info_types = ["Pipeline Info", "Financial Info", "Personell Info", "MAndA", "Layoffs", "NewHires", "TherapyApproval", "IndicationChange", "EarningsReport"]

        # Combine all into one list
        companies_categories_info_types = companies + categories + info_types

        # Prepare a dictionary to hold company, category, and info type values
        columns_and_values = {}

        for field in companies_categories_info_types:
            # Ensure the field name is correctly formatted for SQL
            field_formatted = f"[{field}]"
            columns_and_values[field_formatted] = data.get(field, False)  # Default to False if not found

        # Extract the Company_Name from the old_object and new_object
        old_company_name = old_object.get("Company_Name", "")
        new_company_name = new_object.get("Company_Name", "")

        # Update Stream table if Company_Name exists as a column
        for company_name in [old_company_name, new_company_name]:
            if company_name and company_name in companies:
                columns_and_values[f"[{company_name}]"] = True  # Set the corresponding column to True

        # Add old_object and new_object to the columns and values
        columns_and_values["old_object"] = json.dumps(old_object)  # Convert old_object to JSON string
        columns_and_values["new_object"] = json.dumps(new_object)  # Convert new_object to JSON string

        # Construct the SQL insert query
        columns = ', '.join(columns_and_values.keys()) + ', priority, description, timestamp'
        placeholders = ', '.join(['?' for _ in columns_and_values]) + ', ?, ?, ?'
        values = list(columns_and_values.values()) + [priority, description, timestamp]

        insert_query = f"INSERT INTO Stream ({columns}) VALUES ({placeholders})"
        
        # Execute the SQL query with the extracted values
        cursor.execute(insert_query, values)
        conn.commit()

        matched_clients = match_clients_with_notification(conn, response)

        if len(matched_clients) == 0:
            send_sms("9144334333", description)
        else:
            for client in matched_clients:
                # Example fields for notification preferences
                if client.get("Email"):
                    send_email(client["Email"], "New Notification", description)
                if client.get("text"):
                    send_sms(client["text"], description)
                if client.get("Call"):
                    send_call(client["Phone"], description)

    except json.JSONDecodeError:
        # If JSON parsing fails, store the raw response and the objects
        insert_query = "INSERT INTO Stream (raw_response, old_object, new_object) VALUES (?, ?, ?)"
        cursor.execute(insert_query, (response, json.dumps(old_object), json.dumps(new_object)))
        conn.commit()

    finally:
        cursor.close()


def match_clients_with_notification(conn, response):
    cursor = conn.cursor()

    # Modify cursor to return dictionaries
    cursor.execute("SELECT * FROM Notification_Request_Object_Table")
    columns = [desc[0] for desc in cursor.description]

    # Fetch all clients and their preferences
    clients = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # List to store clients who should receive the notification
    matched_clients = []

    # Extract relevant fields from the response
    response_companies = [company for company in columns if response.get(company, False)]
    response_info_types = [info_type for info_type in ["MAndA", "Layoffs", "NewHires", "TherapyApproval", "IndicationChange", "EarningsReport"] if response.get(info_type, False)]
    response_priority = response.get("priority", 1)

    # Iterate through each client
    for client in clients:
        client_priority = client.get('priority', 1)
        match_found = False

        # Check if client has matching preferences for companies and info types
        if any(client.get(company) for company in response_companies) and any(client.get(info_type) for info_type in response_info_types):
            # Check if the priority level matches or is greater
            if response_priority >= client_priority:
                match_found = True

        # If a match is found, add the client to the list
        if match_found:
            matched_clients.append(client)

    cursor.close()
    return matched_clients


def send_email(email_address, subject, message):
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login("your_email@gmail.com", "your_password")
            server.sendmail("your_email@gmail.com", email_address, f"Subject: {subject}\n\n{message}")
        print(f"Email sent to {email_address}")
    except Exception as e:
        print(f"Failed to send email to {email_address}: {e}")

def send_sms(phone_number, message):
    try:
        client = Client("ACe972b1c490584f6636c1421231661770", "8951a5aefa3eeaba361a5deaf0eb8fbf")
        client.messages.create(body=message, from_="+15512136764", to=phone_number)
        print(f"SMS sent to {phone_number}")
    except Exception as e:
        print(f"Failed to send SMS to {phone_number}: {e}")

def send_call(phone_number, message):
    try:
        client = Client("account_sid", "auth_token")
        call = client.calls.create(twiml=f'<Response><Say>{message}</Say></Response>', to=phone_number, from_="+123456789")
        print(f"Call made to {phone_number}")
    except Exception as e:
        print(f"Failed to make call to {phone_number}: {e}")


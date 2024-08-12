from flask import Blueprint, jsonify
import pyodbc
import json
import logging
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
import openai

# Define a Blueprint
notifications_bp = Blueprint('notifications', __name__)

@notifications_bp.route('/trigger-notifications', methods=['GET'])
def trigger_notifications():
    conn = None
    cursor = None
    try:
        # Initialize OpenAI API and LLM
        openai.api_key = "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"
        llm = ChatOpenAI(temperature=0, model="gpt-4")
        
        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["old_object", "new_object"],
            template="""
                Compare the following two JSON objects and provide a priority of change on a scale of 1-5 (1 being not important and 5 being extremely important).
                Also, provide a description of the change in a concise manner.

                Old Object:
                {old_object}

                New Object:
                {new_object}

                Return the response in the following JSON format:
                {{
                    "priority": 1-5,
                    "description": "description of the change"
                }}
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

        # Fetch all treatment data
        cursor.execute("SELECT Treatment_Key, Treatment_Data FROM Revised_MasterTable")
        all_data = cursor.fetchall()

        # Convert data to dictionaries for easier comparison
        data_dict = {row[0]: json.loads(row[1]) for row in all_data}

        # Compare records and update notifications
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

                # Parse and use the response
                try:
                    change_info = json.loads(response.content)
                    priority = change_info.get("priority", 1)
                    description = change_info.get("description", "No significant changes detected")

                    # Add the notification description to the App_Notification
                    latest_details['App_Notification'] = json.dumps([{"en": description}], ensure_ascii=False)

                except json.JSONDecodeError:
                    logging.error("Failed to parse the response as JSON.")
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
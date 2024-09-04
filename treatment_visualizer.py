import pyodbc
from flask import Blueprint, jsonify, request

fetch_treatments_bp = Blueprint('fetch_treatments', __name__)

server = 'scrapedtreatmentsdatabase.database.windows.net'
database = 'scrapedtreatmentssqldatabase'
username = 'mzandi'
password = 'Ranger22!'
driver = '{ODBC Driver 17 for SQL Server}'
connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    conn = pyodbc.connect(connection_string)
    return conn

@fetch_treatments_bp.route('/api/treatments/search', methods=['GET'])
def get_treatments():
    conn = get_db_connection()
    cursor = conn.cursor()

    search_term = request.args.get('searchTerm', '').lower()
    search_by = request.args.get('searchBy', 'treatment_name').lower()
    companies = request.args.get('companies', '').split(',')

    try:
        # Fetch all treatments
        cursor.execute("SELECT Company_Name, Treatment_Key, Treatment_Data FROM Revised_MasterTable")
        rows = cursor.fetchall()

        # Organize treatments by phase, including Registration
        treatments_by_phase = {
            "Phase 1": [],
            "Phase 2": [],
            "Phase 3": [],
            "Registration": [],
            "Other": []
        }

        for row in rows:
            company_name = row.Company_Name
            treatment_key = row.Treatment_Key
            treatment_data_list = eval(row.Treatment_Data)  # Convert string to list of dictionaries

            # Filter by selected companies if any are specified
            if companies and companies[0] and company_name not in companies:
                continue  # Skip if company is not in the selected list

            # If searching by company name, we can directly compare
            if search_by == 'company_name':
                if search_term not in company_name.lower():
                    continue  # Skip if company name doesn't match

            if treatment_data_list:
                # Get the most recent treatment data
                most_recent_data = treatment_data_list[-1]  # Assuming the last entry is the most recent
                most_recent_data = list(most_recent_data.values())[0]  # Extract actual data

                # Extract relevant details
                phase = most_recent_data.get("Phase", "Other").lower()  # Default to "Other" if phase is missing
                treatment_name = most_recent_data.get("Treatment_Name", "Unknown Treatment").lower()
                target = most_recent_data.get("Target", "Unknown Target").lower()
                indication = most_recent_data.get("Indication", "Unknown Indication").lower()

                # Apply search filtering for non-company searches
                if search_by != 'company_name':
                    search_fields = {
                        'treatment_name': treatment_name,
                        'target': target,
                        'phase': phase,
                        'indication': indication,
                        'company_name': company_name
                    }

                    if search_term and search_term not in search_fields.get(search_by, ''):
                        continue  # Skip this treatment if it doesn't match the search term

                treatment_info = {
                    "treatment_name": most_recent_data.get("Treatment_Name", "Unknown Treatment"),
                    "company_name": company_name,
                    "target": most_recent_data.get("Target", "Unknown Target"),
                    "indication": most_recent_data.get("Indication", "Unknown Indication"),
                }

                # Classify based on phase, including Registration
                if phase in ["phase 1", "phase 2", "phase 3", "registration"]:
                    treatments_by_phase[phase.capitalize()].append(treatment_info)
                else:
                    # Include the phase for treatments classified as "Other"
                    treatment_info["phase"] = most_recent_data.get("Phase", "Other").capitalize()
                    treatments_by_phase["Other"].append(treatment_info)

        return jsonify(treatments_by_phase), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()

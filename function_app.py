import azure.functions as func
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import pyodbc
import requests
import re
from cleanup_phase import clean_phase
from cleanup_text import clean_text
import json


class MasterTable:
    def __init__(self, company_name="Null", therapeutic_area="Null", treatment_name="Null", target="Null",
                 type_of_molecule="Null",
                 indication="Null", phase="Null", date_last_changed="Null", date_scraped="Null",
                 identification_key="Null", modality="Null", brand_name="Null", filing_date="Null",
                 submission_type="Null", notes="Null", disease_area="Null", phase_commencement_date="Null"):
        self.Company_Name = company_name
        self.Therapeutic_Area = therapeutic_area
        self.Treatment_Name = treatment_name
        self.Target = target
        self.Type_of_Molecule = type_of_molecule
        self.Indication = indication
        self.Phase = phase
        self.Date_Last_Changed = date_last_changed
        self.Date_Scraped = date_scraped
        self.Identification_Key = identification_key
        self.Modality = modality
        self.Brand_Name = brand_name
        self.Filing_Date = filing_date
        self.Submission_Type = submission_type
        self.Notes = notes
        self.Disease_Area = disease_area
        self.Phase_Commencement_Date=phase_commencement_date

def fetch_with_zyte(url):
    api_key = '674ce168a19e42d9ac0e039e9d9ded63'

    # Define the request parameters
    request_params = {
        "url": url,
        "browserHtml": True,
        "actions": [
            {"action": "scrollBottom"},  # Scroll to the bottom to trigger loading
            {"action": "waitForTimeout", "timeout": 15}  # Wait for 5000 ms after scroll
        ]
    }

    # Make the API request
    api_response = requests.post(
        "https://api.zyte.com/v1/extract",
        auth=(api_key, ""),
        json=request_params
    )

    # Process the API response
    if api_response.ok:
        browser_html = api_response.json()["browserHtml"]
        return browser_html
    else:
        print(f"Failed to retrieve HTML for {url}. Status code: {api_response.status_code}")
        print("Response:", api_response.text)


class MultilingualData:
    def __init__(self, data=None):
        if data is None:
            self.data = {}
        else:
            self.data = data

    def add_translation(self, language, value):
        self.data[language] = value

    def get_translations_as_dict(self):
        return self.data

    def has_translation(self, language):
        return language in self.data

    def translate_and_add(self, translation_func):
        if 'en' in self.data:
            logging.info("English translation already exists. No translation needed.")
            return None  # No need to return a list

        source_lang = next(iter(self.data))
        source_text = self.data[source_lang]
        translated_text = translation_func(source_text, source_lang, 'en')
        return {'en': translated_text}  # Return a dictionary directly
    

class MultilingualDataCollection:
    def __init__(self):
        self.collection = []

    def add_data(self, multilingual_data):
        if isinstance(multilingual_data, MultilingualData):
            self.collection.append(multilingual_data.get_translations_as_dict())

    def add_translations(self, translations):
        for translation in translations:
            if isinstance(translation, MultilingualData):
                self.add_data(translation)

    def get_collection_as_json(self):
        return json.dumps(self.collection, ensure_ascii=False)

    def find_by_language_and_text(self, language, text):
        return [data for data in self.collection if data.get(language) == text]

    def sort_by_language(self, language):
        self.collection.sort(key=lambda data: data.get(language, ""))


def translate_text(text, source_lang='fr', target_lang='en'):
    translated_text = call_translation_api(text, source_lang, target_lang)
    return translated_text

def call_translation_api(text, source_lang, target_lang):
    return f"Translated {text} from {source_lang} to {target_lang}"


def process_and_translate_row(treatment_data, cursor, treatment_key):
    try:
        if isinstance(treatment_data, list):
            treatment_details = treatment_data[-1]  # Latest entry
        else:
            treatment_details = treatment_data

        today = datetime.now().strftime('%Y%m%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        # Fetch the latest data for today and yesterday
        latest_date_key = max(treatment_details.keys())
        latest_details = treatment_details[latest_date_key]

        # Attempt to fetch the previous day's data
        previous_details = treatment_details.get(yesterday, {})

        for field in ["Therapeutic_Area", "Target", "Indication", "App_Notification"]:
            if field in latest_details:
                current_field_data = json.loads(latest_details[field])  # Current day's data
                logging.info(f'Todays data {current_field_data}')
                previous_field_data = json.loads(previous_details.get(field, '[]'))  # Previous day's data
                logging.info("English translation already exists. No translation needed.")


                multilingual_data = MultilingualData(current_field_data[0] if current_field_data else {})

                if multilingual_data.get_translations_as_dict() != (previous_field_data[0] if previous_field_data else {}):
                    new_translation = multilingual_data.translate_and_add(translate_text)
                    if new_translation:  # Check if a new translation was added
                        current_field_data.append(new_translation)  # Append new dictionary
                        logging.info(f"New translation added for {field} in {treatment_key}")
                else:
                    logging.info(f"Reusing previous translation for {field} in {treatment_key}")

                latest_details[field] = json.dumps(current_field_data, ensure_ascii=False)  # Update with new list

        updated_json = json.dumps(treatment_details, ensure_ascii=False)
        update_query = "UPDATE Revised_MasterTable SET Treatment_Data = ? WHERE Treatment_Key = ?"
        cursor.execute(update_query, (updated_json, treatment_key))

    except Exception as e:
        logging.error(f"Error processing treatment with key {treatment_key}: {e}")
        raise

def abbvie_pipeline():
    try:
        web = "https://www.abbvie.com/science/pipeline.html"
        html_content = fetch_with_zyte(web)

        treatments = []
        processed_treatments = set()

        soup = BeautifulSoup(html_content, 'html.parser')
        pipeline_items = soup.find_all('div', class_='cmp-pipeline')
        for item in pipeline_items:
            therapeutic_area = item['data-asset-focus-area']
            name = item['data-title']
            target = item['data-asset-target']
            type_of_molecule = item['data-asset-type']
            phase_elements = item.find_all('div', class_='phase-element')

            for phase_element in phase_elements:
                phases_containers = phase_element.find_all('div', class_='phases-container')
                for container in phases_containers:
                    indication = container.find('div', class_='col1').text.strip()
                    phase_div = container.find('div', class_='col3')
                    phase_class = phase_div.find('div', class_='bar')['class'][-1]
                    phase = clean_phase(phase_class)

                    indication = clean_text(indication)
                    therapeutic_area = clean_text(therapeutic_area)
                    name = clean_text(name)
                    target = clean_text(target)
                    type_of_molecule = clean_text(type_of_molecule)

                    # Generate identification key
                    identification_key = generate_identification_key("AbbVie", name, indication)

                    therapeutic_area_translator = MultilingualData()
                    target_translator = MultilingualData()
                    type_of_molecule_translator = MultilingualData()
                    indication_translator = MultilingualData()

                    therapeutic_area_translator.add_translation("en", therapeutic_area)
                    target_translator.add_translation("en", target)
                    type_of_molecule_translator.add_translation("en", type_of_molecule)
                    indication_translator.add_translation("en", indication)

                    therapeutic_area_collection = MultilingualDataCollection()
                    target_collection = MultilingualDataCollection()
                    type_of_molecule_collection = MultilingualDataCollection()
                    indication_collection = MultilingualDataCollection()

                    therapeutic_area_collection.add_data(therapeutic_area_translator)
                    target_collection.add_data(target_translator)
                    type_of_molecule_collection.add_data(type_of_molecule_translator)
                    indication_collection.add_data(indication_translator)

                    treatment_key = (therapeutic_area, name, target, type_of_molecule, indication, phase)
                    if treatment_key not in processed_treatments:
                        master_record = MasterTable(
                            company_name="AbbVie",
                            therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                            treatment_name=name,
                            target=target_collection.get_collection_as_json(),
                            type_of_molecule=type_of_molecule_collection.get_collection_as_json(),
                            indication=indication_collection.get_collection_as_json(),
                            phase=phase,
                            date_scraped=datetime.now(timezone.utc),
                            identification_key=identification_key
                        )
                        treatments.append(master_record.__dict__)
                        processed_treatments.add(treatment_key)

        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping AbbVie's Pipeline: ", e)
        return [], None


def bayer_pipeline():
    try:
        web = "https://www.bayer.com/en/pharma/development-pipeline"
        html_content = fetch_with_zyte(web)

        treatments = []
        processed_treatments = set()

        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the relevant table or data area
        pipeline_items = soup.find_all('tr')  # Adjust the selector as needed

        for row in pipeline_items:
            cells = row.find_all('td')
            if len(cells) >= 4:
                phase = cells[0].text.strip()
                area = cells[1].text.strip().replace('\n', ' ')
                program_mode_of_action = cells[2].text.strip().replace('\n', ' ')
                indication = cells[3].text.strip().replace('\n', ' ')

                # Assume generate_identification_key and clean_text are defined elsewhere
                identification_key = generate_identification_key("Bayer", program_mode_of_action, indication)

                treatment_key = (area, program_mode_of_action, indication, phase)

                area_translator = MultilingualData()
                program_mode_of_action_translator = MultilingualData()
                indication_translator = MultilingualData()

                area_translator.add_translation("en", area)
                program_mode_of_action_translator.add_translation("en", program_mode_of_action)
                indication_translator.add_translation("en", indication)

                area_collection = MultilingualDataCollection()
                program_mode_of_action_collection = MultilingualDataCollection()
                indication_collection = MultilingualDataCollection()

                area_collection.add_data(area_translator)
                program_mode_of_action_collection.add_data(program_mode_of_action_translator)
                indication_collection.add_data(indication_translator)


                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Bayer",
                        therapeutic_area=area_collection.get_collection_as_json(),
                        treatment_name=program_mode_of_action_collection.get_collection_as_json(),
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        date_scraped=datetime.now(timezone.utc),
                        identification_key=identification_key  # Set the generated key
                    )
                    treatments.append(master_record.__dict__)
                    processed_treatments.add(treatment_key)

        return treatments, html_content
    except Exception as e:
        print("An error occurred processing Bayer Therapeutics Pipeline data: ", e)
        return []


def boehringer_ingelheim_pipeline():
    try:
        url = "https://www.boehringer-ingelheim.com/boehringer-ingelheim-human-pharma-clinical-pipeline-dynamic"
        html_content = fetch_with_zyte(url)
        soup = BeautifulSoup(html_content, 'html.parser')

        treatments = []
        processed_treatments = set()

        elements = soup.find_all(class_='text_combine')

        for element in elements:
            try:
                treatment_name = element.find(class_='box_heading').get_text(strip=True)
                indication = element.find(class_='box_sub_heading').get_text(strip=True)
                therapeutic_area = element.find(class_='ta').get_text(strip=True)

                if "|" in indication:
                    target, indication = indication.split("|", 1)
                else:
                    target = "Null"

                target = target.strip()
                indication = indication.strip() if indication else "Null"

                # Attempt to find the closest ancestor with class 'flex_phase'
                parent_element = element.find_previous(class_='flex_phase')
                if parent_element:
                    phase_initial = parent_element.find(class_='phase_title').get_text(strip=True)
                    phase = clean_phase(phase_initial)
                else:
                    phase = "Unknown"  # Default or error handling

                # Clean and prepare data
                therapeutic_area = clean_text(therapeutic_area)
                treatment_name = clean_text(treatment_name)
                indication = clean_text(indication)

                date_scraped = datetime.now(timezone.utc)
                identification_key = generate_identification_key("Boehringer Ingelheim", treatment_name, indication)
                treatment_key = (therapeutic_area, treatment_name, indication, phase)

                therapeutic_area_translator = MultilingualData()
                target_translator = MultilingualData()
                indication_translator = MultilingualData()

                therapeutic_area_translator.add_translation("en", therapeutic_area)
                target_translator.add_translation("en", target)
                indication_translator.add_translation("en", indication_translator)

                therapeutic_area_collection = MultilingualDataCollection()
                target_collection = MultilingualDataCollection()
                indication_collection = MultilingualDataCollection()

                therapeutic_area_collection.add_data(therapeutic_area_translator)
                target_collection.add_data(target_translator)
                indication_collection.add_data(indication_collection)

                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Boehringer Ingelheim",
                        therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                        treatment_name=treatment_name,
                        target=target_collection.get_collection_as_json(),
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        date_scraped=date_scraped,
                        identification_key=identification_key
                    )
                    treatments.append(master_record.__dict__)  # Append dictionary if needed for DB operations
                    processed_treatments.add(treatment_key)  # Track processed entries

            except Exception as e:
                print(f"Error extracting data for element: {e}")

        return treatments, html_content
    except Exception as e:
        print(f"An error occurred scraping Boehringer Ingelheim's pipeline: {e}")
        return []


def bms_pipeline():
    url = "https://www.bms.com/researchers-and-partners/in-the-pipeline.html"
    html_content = fetch_with_zyte(url)
    soup = BeautifulSoup(html_content, 'html.parser')

    treatments = []
    processed_treatments = set()

    date_element = soup.select_one(".page-callout .body-1")
    date_last_changed = date_element.text.strip() if date_element else "Date not found"

    # Initialize a list to hold all treatment data
    treatment_data = []
    previous_treatment = ''
    current_header = ''
    current_subheader = ''

    # Extract details for each treatment
    for element in soup.find_all(True):  # Iterate over all elements
        if 'category-heading' in element.get('class', []):  # Check for header
            current_header = element.get_text(strip=True)
            current_subheader = ''  # Reset subheader when a new header is found
        elif 'sub-category-heading' in element.get('class', []):  # Check for subheader
            current_subheader = element.get_text(strip=True)

        if 'pipeline-listing' in element.get('class', []):
            # Initialize a dictionary for this treatment
            treatment_info = {
                'Header': current_header,
                'Subheader': current_subheader if current_subheader else current_header
                # Use subheader if available, else header
            }

            # Get compound name
            name_block = element.find('div', class_='pipeline-data')
            if name_block:
                treatment_info['Compound Name'] = name_block.get_text(strip=True)

            # Get therapeutic area
            therapy_block = element.find('div', class_='pipeline-data-block-opacity-text')
            if therapy_block:
                treatment_info['Therapeutic Area'] = therapy_block.get_text(strip=True)

            # Get all phases associated with this treatment
            phases = element.find_all('div', class_='phase-listing')
            phase_info = [phase.get_text(strip=True) for phase in phases]
            treatment_info["Phase"] = len(phase_info)

            if treatment_info['Compound Name'] == treatment_info['Therapeutic Area']:
                treatment_info['Compound Name'] = previous_treatment

            previous_treatment = treatment_info['Compound Name']

            treatment_info['date_last_changed'] = date_last_changed

            brand_name_compound = treatment_info['Compound Name']
            therapeutic_area = treatment_info['Header']
            disease_area = treatment_info['Subheader']
            phase = treatment_info['Phase']
            research_area_line_of_therapy = treatment_info['Therapeutic Area']

            brand_name_compound = clean_text(brand_name_compound)
            therapeutic_area = clean_text(therapeutic_area)
            disease_area = clean_text(disease_area)
            phase = str(clean_phase(phase))
            research_area_line_of_therapy = clean_phase(research_area_line_of_therapy)


            # Add this treatment's information to the main list
            identification_key = generate_identification_key("Bristol-Myers Squibb", brand_name_compound,
                                                             research_area_line_of_therapy)
            treatment_key = (therapeutic_area, disease_area, brand_name_compound, phase, research_area_line_of_therapy)

            therapeutic_area_translator = MultilingualData()
            research_area_line_of_therapy_translator = MultilingualData()
            disease_area_translator = MultilingualData()

            therapeutic_area_translator.add_translation("en", therapeutic_area)
            research_area_line_of_therapy_translator.add_translation("en", research_area_line_of_therapy)
            disease_area_translator.add_translation("en", disease_area)

            therapeutic_area_collection = MultilingualDataCollection()
            research_area_line_of_therapy_collection = MultilingualDataCollection()
            disease_area_collection = MultilingualDataCollection()

            therapeutic_area_collection.add_data(therapeutic_area_translator)
            research_area_line_of_therapy_collection.add_data(research_area_line_of_therapy_translator)
            disease_area_collection.add_data(disease_area_translator)

            if treatment_key not in processed_treatments:
                date_scraped = datetime.now(timezone.utc)
                master_record = MasterTable(
                    company_name="Bristol-Myers Squibb",
                    therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                    treatment_name=brand_name_compound,
                    indication=research_area_line_of_therapy_collection.get_collection_as_json(),
                    phase=phase,
                    date_scraped=date_scraped,
                    date_last_changed=date_last_changed,
                    identification_key=identification_key,
                    disease_area=disease_area_collection.get_collection_as_json()
                )
                treatments.append(master_record.__dict__)
                processed_treatments.add(treatment_key)
    return treatments, html_content


def gilead_pipeline():
    try:
        url = "https://www.gilead.com/science-and-medicine/pipeline"
        html_content = fetch_with_zyte(url)
        soup = BeautifulSoup(html_content, 'html.parser')

        treatments = []
        processed_treatments = set()

        div_content = soup.find("div", class_="headline-paragraph mt-0")
        date_paragraph = div_content.find("p", class_="body-xs-req") if div_content else None

        # Extract text and store in date_last_changed if the paragraph is found
        date_last_changed = date_paragraph.text.strip() if date_paragraph else "Date not found"

        # Find all accordion-wrapper sections
        accordion_wrappers = soup.find_all("div", class_="pipeline-accordion-wrapper pipeline-result-wrapper")

        # Iterate over each accordion-wrapper
        for wrapper in accordion_wrappers:
            # Find the therapeutic area from the category name
            therapeutic_area = wrapper.find("h2", class_="category-name").text.strip()

            # Find all accordion items within this section
            accordion_items = wrapper.find_all("div", class_="accordion-item")

            # Extract treatment information from accordion items
            for item in accordion_items:
                try:
                    treatment_name = item.find("h5", class_="child-category-name").text.strip() if item.find("h5",
                                                                                                             class_="child-category-name") else "N/A"
                    indication = item.find("p", class_="category-desc").text.strip() if item.find("p",
                                                                                                  class_="category-desc") else "N/A"
                    phase_info = item.find("div", class_="phase-info").text.strip() if item.find("div",
                                                                                                 class_="phase-info") else "N/A"

                    notes_container = item.find("div", class_="accordion-body")

                    notes = ' '.join(notes_container.stripped_strings) if notes_container else "No additional notes"
                except AttributeError:
                    treatment_name = "N/A"
                    indication = "N/A"
                    phase_info = "N/A"

                if phase_info == "Phase 3":
                    phase_info = 3
                elif phase_info == "Phase 2":
                    phase_info = 2
                elif phase_info == "Phase 1":
                    phase_info = 1

                therapeutic_area = clean_text(therapeutic_area)
                treatment_name = clean_text(treatment_name)
                indication = clean_text(indication)
                phase = clean_phase(phase_info)
                notes = clean_text(notes)

                date_scraped = datetime.now(timezone.utc)

                identification_key = generate_identification_key("Gilead Sciences", treatment_name,
                                                                 therapeutic_area)

                treatment_key = (therapeutic_area, treatment_name, phase, indication, notes)

                therapeutic_area_translator = MultilingualData()
                indication_translator = MultilingualData()
                notes_translator = MultilingualData()

                therapeutic_area_translator.add_translation("en", therapeutic_area)
                indication_translator.add_translation("en", indication)
                notes_translator.add_translation("en", notes)

                therapeutic_area_collection = MultilingualDataCollection()
                indication_collection = MultilingualDataCollection()
                notes_collection = MultilingualDataCollection()

                therapeutic_area_collection.add_data(therapeutic_area_translator)
                indication_collection.add_data(indication_translator)
                notes_collection.add_data(notes_translator)

                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Gilead Sciences",
                        therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                        treatment_name=treatment_name,
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        notes=notes_collection.get_collection_as_json(),
                        date_scraped=date_scraped,
                        identification_key=identification_key,
                        date_last_changed=date_last_changed
                    )
                    treatments.append(master_record.__dict__)  # Append dictionary if needed for DB operations
                    processed_treatments.add(treatment_key)  # Track processed entries
        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping Gilead Sciences pipeline", e)
        return [], None


def gsk_pipeline():
    try:
        url = "https://www.gsk.com/en-gb/innovation/pipeline/"
        html_content = fetch_with_zyte(url)
        soup = BeautifulSoup(html_content, 'html.parser')

        # Therapy area color mapping
        therapy_area_mapping = {
            "#244ea2": "Infectious Diseases",
            "#e21860": "HIV",
            "#ffc709": "Respiratory / Immunology",
            "#69b445": "Oncology",
            "#6658a6": "Opportunity Driven"
        }

        treatments = []
        processed_treatments = set()

        pipeline_update_info = soup.select_one('h3.pipeline-info__title')
        date_last_changed = pipeline_update_info.text if pipeline_update_info else "Not available"

        initial_name = ""
        rows = soup.select('tr.compounds-table__row')  # Updated selector
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                text_parts = []
                for p in cells[0].find_all('p', class_='compounds-table__cell-text'):
                    text_parts.append(p.get_text(strip=True))
                    initial_name = ' '.join(text_parts)

                compound_number_generic_name_brand_name = clean_text(initial_name)

                phase = clean_phase(cells[2].text)
                therapy_area_color = cells[0]['data-therapy-area']
                therapeutic_area = therapy_area_mapping.get(therapy_area_color.strip(), "Unknown")
                mode_of_action_vaccine_type = clean_text(cells[3].text)
                indication = clean_text(cells[1].text)

                if mode_of_action_vaccine_type == "":
                    break

                identification_key = generate_identification_key("GSK", compound_number_generic_name_brand_name,
                                                                 indication)
                date_scraped = datetime.now(timezone.utc)

                treatment_key = (therapeutic_area, compound_number_generic_name_brand_name, phase, indication)

                therapeutic_area_translator = MultilingualData()
                indication_translator = MultilingualData()
                mode_of_action_vaccine_type_translator = MultilingualData()

                therapeutic_area_translator.add_translation("en", therapeutic_area)
                indication_translator.add_translation("en", indication)
                mode_of_action_vaccine_type_translator.add_translation("en", mode_of_action_vaccine_type)

                therapeutic_area_collection = MultilingualDataCollection()
                indication_collection = MultilingualDataCollection()
                mode_of_action_vaccine_type_collection = MultilingualDataCollection()

                therapeutic_area_collection.add_data(therapeutic_area_translator)
                indication_collection.add_data(therapeutic_area_translator)
                mode_of_action_vaccine_type_collection.add_data(mode_of_action_vaccine_type_translator)



                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="GSK plc",
                        therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                        treatment_name=compound_number_generic_name_brand_name,
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        target=mode_of_action_vaccine_type_collection.get_collection_as_json(),
                        date_scraped=date_scraped,
                        identification_key=identification_key,
                        date_last_changed=date_last_changed
                    )
                    treatments.append(master_record.__dict__)  # Append dictionary if needed for DB operations
                    processed_treatments.add(treatment_key)  # Track processed entries

        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping GSK's pipeline:", e)
        return [], None


def johnson_johnson_pipeline():
    try:
        url = "https://www.investor.jnj.com/pipeline/development-pipeline/default.aspx"
        html_content = fetch_with_zyte(url)

        soup = BeautifulSoup(html_content, 'html.parser')
        treatments = []
        processed_treatments = set()

        areas = soup.select("section.pipeline-area")
        for area in areas:
            area_name = area.select_one("h2.pipeline-area_title").text.strip()

            cards = area.select("li.pipeline-area_card")
            for card in cards:
                treatment_name = card.select_one("h3.pipeline-area_card-title").text.strip()
                indication = card.select_one("p.pipeline-area_card-description").text.strip()
                phase_initial = card.select_one("p.pipeline-area_card-phase").text.strip()

                phase = clean_phase(phase_initial)

                # Generate a unique identification key for each treatment
                identification_key = generate_identification_key("Johnson_Johnson", treatment_name, indication)
                date_scraped = datetime.now(timezone.utc)

                # Create a unique tuple key for tracking processed treatments
                treatment_key = (area_name, treatment_name, phase, indication)

                therapauetic_area_translator = MultilingualData()
                indication_translator = MultilingualData()

                therapauetic_area_translator.add_translation("en", area_name)
                indication_translator.add_translation("en", indication)

                therapauetic_area_collection = MultilingualDataCollection()
                indication_collection = MultilingualDataCollection()

                therapauetic_area_collection.add_data(therapauetic_area_translator)
                indication_collection.add_data(indication_translator)

                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Johnson&Johnson",
                        therapeutic_area=therapauetic_area_collection.get_collection_as_json(),
                        treatment_name=treatment_name,
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        date_scraped=date_scraped,
                        identification_key=identification_key
                    )
                    treatments.append(master_record.__dict__)  # Append dictionary if needed for DB operations
                    processed_treatments.add(treatment_key)  # Track processed entries

        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping Johnson & Johnson's pipeline:", e)
        return [], None


def merck_pipeline():
    try:
        url = "https://www.merck.com/research/product-pipeline/"
        html_content = fetch_with_zyte(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            caption = soup.find('div', class_='pipeline-caption')
            if caption:
                caption_text = caption.text
                # Regular expression to find the date pattern
                match = re.search(r"Updated\s(\w+\s\d+,\s\d{4})", caption_text)
                if match:
                    date_last_changed = match.group(1)
            else:
                date_last_changed = "No update date found"

            treatments = []
            processed_treatments = set()  # Set to track unique treatments

            treatment_rows = soup.find_all('tr', class_='pipeline-program')
            for row in treatment_rows:
                molecule_name = clean_text_merck(row.find('h4', class_='pipeline-program-name').text)
                therapeutic_area = clean_text_merck(row.find('div', class_='pipeline-program-t-area').find('span').text)
                mechanism_of_action = clean_text_merck(row.find('div', class_='pipeline-program-content').text)
                modality = clean_text_merck(row.find('div', class_='pipeline-program-modality').find('span').text)

                indications_data = row.find_all('tr', class_='pipeline-program-indication')
                for indication in indications_data:
                    indication_final = clean_text_merck(
                        indication.find('h6', class_='pipeline-program-indication-title').text)
                    phase_text = ""

                    phase_bars = indication.find('td', class_='phase-bars-table-data')
                    if phase_bars:
                        under_review = phase_bars.find('h6', class_='pipeline-program-indication-title')
                        phase_text = clean_text_merck(
                            under_review.text) if under_review and 'Under review' in under_review.text else ""

                    if not phase_text:
                        phase_bars = indication.find('div', class_='pipeline-phase-bars')
                        if phase_bars:
                            phase_count = len(phase_bars.find_all('div', class_='pipeline-phase-bar active'))
                            phase_text = clean_phase(phase_count) if phase_count > 0 else ""

                    molecule_name = clean_text(molecule_name)
                    therapeutic_area = clean_text(therapeutic_area)
                    indication_final = clean_text(indication_final)
                    phase_text = clean_text(phase_text)
                    mechanism_of_action = clean_text(mechanism_of_action)
                    modality = clean_text(modality)

                    # Generate a unique identification key for each treatment
                    identification_key = generate_identification_key("Merck", molecule_name, indication_final)
                    date_scraped = datetime.now(timezone.utc)

                    # Generate a unique identification key
                    treatment_key = (
                    molecule_name, therapeutic_area, indication_final, phase_text, mechanism_of_action, modality)

                    therapeutic_area_translator = MultilingualData()
                    indication_area_translator = MultilingualData()
                    mechanism_of_action_translator = MultilingualData()
                    modality_translator = MultilingualData()

                    therapeutic_area_translator.add_translation("en", therapeutic_area)
                    indication_area_translator.add_translation("en", indication_final)
                    mechanism_of_action_translator.add_translation("en", mechanism_of_action)
                    modality_translator.add_translation("en", modality)

                    therapeutic_area_collection = MultilingualDataCollection()
                    indication_collection = MultilingualDataCollection()
                    mechanism_of_action_collection = MultilingualDataCollection()
                    modality_collection = MultilingualDataCollection()

                    therapeutic_area_collection.add_data(therapeutic_area_translator)
                    indication_collection.add_data(indication_area_translator)
                    mechanism_of_action_collection.add_data(mechanism_of_action_translator)
                    modality_collection.add_data(modality_translator)

                    if treatment_key not in processed_treatments:
                        master_record = MasterTable(
                            company_name="Merck & Co.",
                            treatment_name=molecule_name,
                            therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                            indication=indication_collection.get_collection_as_json(),
                            phase=phase_text,
                            target=mechanism_of_action_collection.get_collection_as_json(),
                            modality=modality_collection.get_collection_as_json(),
                            identification_key=identification_key,
                            date_last_changed=date_last_changed,
                            date_scraped=date_scraped
                        )
                        treatments.append(master_record.__dict__)  # Append the dictionary representation
                        processed_treatments.add(treatment_key)

            return treatments, html_content
        else:
            print("Failed to retrieve the webpage")
            return None
    except Exception as e:
        print("An error occurred scraping Merck & Co.'s Pipeline", e)
        return [], None


def novartis_pipeline():
    try:
        # Base URL and start page setup
        start_page = '0'
        base_url = 'https://www.novartis.com/research-development/novartis-pipeline?search_api_fulltext=&page='
        current_page = start_page
        all_treatments = []
        final_html = ""

        while True:
            html = fetch_with_zyte(base_url + current_page)
            final_html += html
            treatments = parse_treatments_novartis(html)
            all_treatments.extend(treatments)

            soup = BeautifulSoup(html, 'html.parser')
            next_page_link = soup.select_one('.pager__item--next a')
            if next_page_link and 'href' in next_page_link.attrs:
                # Correctly update current_page to fetch the next page
                current_page = next_page_link['href'].split('=')[-1]  # Assumes URL ends with '?page=n'
                if current_page not in ["1", "2", "3", "4", "5"]:
                    break
            else:
                break

        treatments_final = []
        processed_treatments = set()  # Set to track unique treatments

        for treatment in all_treatments:
            project = treatment["Project"]
            brand_name = treatment["Product"]
            indication = treatment["Indication"]
            therapeutic_area = treatment["Therapeutic_Area"]
            development_phase = treatment["Development_Phase"]
            filing_date = treatment["Filing_Date"]
            mechanism_of_action = treatment["Mechanism_of_Action"]

            project = clean_text(project)
            therapeutic_area = clean_text(therapeutic_area)
            indication = clean_text(indication)
            mechanism_of_action = clean_text(mechanism_of_action)

            identification_key = generate_identification_key("Novartis", project, indication)
            date_scraped = datetime.now(timezone.utc)

            # Generate a unique identification key
            treatment_key = (project, therapeutic_area, indication, development_phase, mechanism_of_action)

            therapeutic_area_translator = MultilingualData()
            indication_translator = MultilingualData()
            mechanism_of_action_translator = MultilingualData()


            therapeutic_area_translator.add_translation("en", therapeutic_area)
            indication_translator.add_translation("en", indication)
            mechanism_of_action_translator.add_translation("en", mechanism_of_action)

            therapeutic_area_collection = MultilingualDataCollection()
            indication_collection = MultilingualDataCollection()
            mechanism_of_action_collection = MultilingualDataCollection()

            therapeutic_area_collection.add_data(therapeutic_area_translator)
            indication_collection.add_data(indication_translator)
            mechanism_of_action_collection.add_data(mechanism_of_action_translator)

            if treatment_key not in processed_treatments:
                master_record = MasterTable(
                    company_name="Novartis",
                    treatment_name=project,
                    therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                    indication=indication_collection.get_collection_as_json(),
                    phase=development_phase,
                    target=mechanism_of_action_collection.get_collection_as_json(),
                    identification_key=identification_key,
                    date_scraped=date_scraped,
                    brand_name=brand_name,
                    filing_date=filing_date
                )
                treatments_final.append(master_record.__dict__)  # Append the dictionary representation
                processed_treatments.add(treatment_key)

        return treatments_final, final_html
    except Exception as e:
        print("An error occurred scraping Novartis's pipeline", e)
        return [], None


def novo_nordisk_pipeline():
    try:
        # URL of the webpage to scrape
        web = "https://www.novonordisk.com/science-and-technology/r-d-pipeline.html"

        # Fetch the webpage content
        response = requests.get(web)
        response.raise_for_status()  # Ensure the request was successful

        html = response.text

        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the section with "phasesgrid"
        pipeline_container = soup.find('div', class_="phasesgrid")

        # Extract the treatment information
        phases = pipeline_container.find_all('div', class_="phase-item")

        treatments = []
        processed_treatments = set()  # Set to track unique treatments

        for phase in phases:
            # Find all the treatments in this phase
            treatment_containers = phase.find('div', class_='area')
            diff_treatments = treatment_containers.find_all('rndarea')
            for treat in diff_treatments:
                treat = str(treat)
                treat_terms = treat.split()
                in_name = False
                final_name = ""
                indication = ""
                phase = ""
                for word in treat_terms:
                    if "area=" in word:
                        in_name = False
                        indication = word.split('"')

                    if ("key=" in word) or (in_name == True):
                        in_name = True
                        final_name += word + " "

                    if "phase=" in word:
                        phase = word.split('"')
                        if "1" in phase[1]:
                            phase = "Phase 1"
                        elif "2" in phase[1]:
                            phase = "Phase 2"
                        elif "3" in phase[1]:
                            phase = "Phase 3"
                        elif "filed" in phase[1]:
                            phase = "filed"

                final_name = final_name.split("'")
                treatment_name = final_name[1]
                indication = indication[1]
                phase = phase

                treatment_name = clean_text(treatment_name)
                indication = clean_text(indication)
                phase = clean_phase(phase)

                identification_key = generate_identification_key("Novo Nordisk", treatment_name, indication)
                date_scraped = datetime.now(timezone.utc)

                # Generate a unique identification key
                treatment_key = (treatment_name, indication, phase)

                indication_translator = MultilingualData()
                indication_translator.add_translation("en", indication)

                indication_collection = MultilingualDataCollection()
                indication_collection.add_data(indication_translator)

                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Novo Nordisk",
                        treatment_name=treatment_name,
                        indication=indication_collection.get_collection_as_json(),
                        phase=phase,
                        identification_key=identification_key,
                        date_scraped=date_scraped
                    )
                    treatments.append(master_record.__dict__)  # Append the dictionary representation
                    processed_treatments.add(treatment_key)

        return treatments, html
    except Exception as e:
        print("An error occurred scraping Novo Nordisk's Pipeline", e)
        return [], None


def pfizer_pipeline():
    try:
        base_url = "https://www.pfizer.com/v1/pipeline/filter"
        html_content = fetch_with_zyte(base_url)

        soup = BeautifulSoup(html_content, 'html.parser')
        date_element = soup.find('p', class_='pipeline-txt-date')
        date_last_changed = clean_text(date_element.text).replace('as of ', '') if date_element else datetime.now(timezone.utc)

        params = {
            'ugcf_project_discontinued[]': 'current',
            'search': '',
            'order': 'ugcf_phase_of_development',
            'limit': '10',
            'page': 1,
            'pager': 'true',
            'style': 'detailed'
        }

        # Initially fetch the first page to get the total page count
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print("Failed to fetch data:", response.status_code)
            return

        html = ""
        initial_data = response.json()
        total_pages = initial_data['data']['page_count']

        treatments = []
        processed_treatments = set()

        # Iterate through all pages and fetch treatments
        for page in range(0, total_pages + 1):
            params['page'] = page
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                html += response.text
                products = data['data']['products']
                for product_id, product_info in products.items():
                    area_of_focus = clean_text(product_info.get('field_ugcf_therapeutic_area', ''))
                    compound_name = clean_text(product_info.get('field_ugcf_compound_name', ''))
                    indication = clean_text(product_info.get('field_ugcf_indication', ''))
                    compound_type = clean_text(product_info.get('field_ugcf_compound_type', ''))
                    phase = clean_text(product_info.get('field_ugcf_phase_of_development', ''))
                    mechanism_of_action = clean_text(product_info.get('field_ugcf_mechanism_of_action', ''))
                    submission_type = clean_text(product_info.get('field_ugcf_submission_type', ''))

                    identification_key = generate_identification_key("Pfizer", compound_name, indication)
                    date_scraped = datetime.now(timezone.utc)

                    treatment_key = (area_of_focus, compound_name, mechanism_of_action, compound_type, indication, phase,
                    submission_type)

                    area_of_focus_translator = MultilingualData()
                    indication_translator = MultilingualData()
                    compound_type_translator = MultilingualData()
                    mechanism_of_action_translator = MultilingualData()
                    submission_type_translator = MultilingualData()

                    area_of_focus_translator.add_translation("en", area_of_focus)
                    indication_translator.add_translation("en", indication)
                    compound_type_translator.add_translation("en", compound_type)
                    mechanism_of_action_translator.add_translation("en", mechanism_of_action)
                    submission_type_translator.add_translation("en", submission_type)

                    area_of_focus_collection = MultilingualDataCollection()
                    indication_collection = MultilingualDataCollection()
                    compound_type_collection = MultilingualDataCollection()
                    mechanism_of_action_collection = MultilingualDataCollection()
                    submission_type_collection = MultilingualDataCollection()

                    area_of_focus_collection.add_data(area_of_focus_translator)
                    indication_collection.add_data(indication_translator)
                    compound_type_collection.add_data(compound_type_translator)
                    mechanism_of_action_collection.add_data(mechanism_of_action_translator)
                    submission_type_collection.add_data(submission_type_translator)

                    if treatment_key not in processed_treatments:
                        master_record = MasterTable(
                            company_name="Pfizer",
                            therapeutic_area=area_of_focus_collection.get_collection_as_json(),
                            treatment_name=compound_name,
                            indication=indication_collection.get_collection_as_json(),
                            type_of_molecule=compound_type_collection.get_collection_as_json(),
                            phase=phase,
                            target=mechanism_of_action_collection.get_collection_as_json(),
                            submission_type=submission_type_collection.get_collection_as_json(),
                            identification_key=identification_key,
                            date_last_changed=date_last_changed,
                            date_scraped=date_scraped
                        )
                        treatments.append(master_record.__dict__)  # Append the dictionary representation
                        processed_treatments.add(treatment_key)
            else:
                print(f"Failed to fetch data for page {page} with status code {response.status_code}")

        return treatments, html
    except Exception as e:
        print("An error occurred scraping Novo Pfizer's Pipeline", e)
        return [], None


def sanofi_pipeline():
    try:
        import requests
        from bs4 import BeautifulSoup

        # URL of the webpage to scrape
        url = 'https://www.sanofi.com/en/our-science/our-pipeline'
        response = requests.get(url)
        response.raise_for_status()  # Check to make sure the request was successful

        html = response.text

        soup = BeautifulSoup(response.text, 'html.parser')

        treatments = []
        processed_treatments = set()  # Set to track unique treatments

        # Finding the specific container for the treatment based on the provided HTML structure
        treatment_containers = soup.select(
            '.MuiGrid2-root.MuiGrid2-container.MuiGrid2-direction-xs-row.css-19oavfy-MuiGrid2-root')

        for container in treatment_containers:
            # Extracting each piece of data based on its specific location and class
            therapeutic_area = container.select_one('.css-14a34ya-MuiTypography-root').text.strip()
            phase = clean_phase(container.select_one('.css-f9uo9l-MuiTypography-root').text.strip())
            name = container.select_one('.css-1d7wlyc-MuiTypography-root').text.strip()
            description = container.select_one('.css-1ubkcfk-MuiTypography-root').text.strip()
            indication = container.select_one('.css-1rwojcg-MuiTypography-root').text.strip()

            name = clean_text(name)
            indication = clean_text(indication)
            therapeutic_area = clean_text(therapeutic_area)
            description = clean_text(description)

            identification_key = generate_identification_key("Sanofi", name, indication)
            date_scraped = datetime.now(timezone.utc)

            # Generate a unique identification key
            treatment_key = (name, indication, phase, therapeutic_area, description)

            indication_translator = MultilingualData()
            therapeutic_area_translator = MultilingualData()
            description_translator = MultilingualData()

            indication_translator.add_translation('en', indication)
            therapeutic_area_translator.add_translation('en', therapeutic_area)
            description_translator.add_translation('en', description)

            indication_collection = MultilingualDataCollection()
            therapeutic_area_collection = MultilingualDataCollection()
            description_collection = MultilingualDataCollection()

            indication_collection.add_data(indication_translator)
            therapeutic_area_collection.add_data(therapeutic_area_translator)
            description_collection.add_data(description_translator)

            if treatment_key not in processed_treatments:
                master_record = MasterTable(
                    company_name="Sanofi",
                    treatment_name=name,
                    indication=str(indication_collection.get_collection_as_json()),
                    therapeutic_area=str(therapeutic_area_collection.get_collection_as_json()),
                    phase=phase,
                    target=str(description_collection.get_collection_as_json()),
                    identification_key=identification_key,
                    date_scraped=date_scraped
                )
                treatments.append(master_record.__dict__)  # Append the dictionary representation
                processed_treatments.add(treatment_key)

        return treatments, html
    except Exception as e:
        print("An error occurred scraping Sanofi's pipeline:", e)
        return [], None

def sanofi_pipeline_french():
    try:
        import requests
        from bs4 import BeautifulSoup

        # URL of the webpage to scrape
        url = 'https://www.sanofi.com/fr/notre-science/notre-portefeuille'
        response = requests.get(url)
        response.raise_for_status()  # Check to make sure the request was successful

        html = response.text

        soup = BeautifulSoup(response.text, 'html.parser')

        treatments = []
        processed_treatments = set()  # Set to track unique treatments

        # Finding the specific container for the treatment based on the provided HTML structure
        treatment_containers = soup.select(
            '.MuiGrid2-root.MuiGrid2-container.MuiGrid2-direction-xs-row.css-19oavfy-MuiGrid2-root')

        for container in treatment_containers:
            # Extracting each piece of data based on its specific location and class
            therapeutic_area = container.select_one('.css-14a34ya-MuiTypography-root').text.strip()
            phase = clean_phase(container.select_one('.css-f9uo9l-MuiTypography-root').text.strip())
            name = container.select_one('.css-1d7wlyc-MuiTypography-root').text.strip()
            description = container.select_one('.css-1ubkcfk-MuiTypography-root').text.strip()
            indication = container.select_one('.css-1rwojcg-MuiTypography-root').text.strip()

            name = clean_text(name)
            indication = clean_text(indication)
            therapeutic_area = clean_text(therapeutic_area)
            description = clean_text(description)

            identification_key = generate_identification_key("Sanofi", name, indication)
            date_scraped = datetime.now(timezone.utc)

            # Generate a unique identification key
            treatment_key = (name, indication, phase, therapeutic_area, description)

            indication_translator = MultilingualData()
            therapeutic_area_translator = MultilingualData()
            description_translator = MultilingualData()

            indication_translator.add_translation('fr', indication)
            therapeutic_area_translator.add_translation('fr', therapeutic_area)
            description_translator.add_translation('fr', description)

            # Add to collections
            indication_collection = MultilingualDataCollection()
            therapeutic_area_collection = MultilingualDataCollection()
            description_collection = MultilingualDataCollection()

            indication_collection.add_data(indication_translator)
            therapeutic_area_collection.add_data(therapeutic_area_translator)
            description_collection.add_data(description_translator)

            if treatment_key not in processed_treatments:
                master_record = MasterTable(
                    company_name="Sanofi",
                    treatment_name=name,
                    indication=str(indication_collection.get_collection_as_json()),
                    therapeutic_area=str(therapeutic_area_collection.get_collection_as_json()),
                    phase=phase,
                    target=str(description_collection.get_collection_as_json()),
                    identification_key=identification_key,
                    date_scraped=date_scraped
                )
                treatments.append(master_record.__dict__)  # Append the dictionary representation
                processed_treatments.add(treatment_key)

        return treatments, html
    except Exception as e:
        print("An error occurred scraping Sanofi's pipeline:", e)
        return [], None



def teva_pipeline():
    try:
        web = "https://www.tevapharm.com/product-focus/research/pipeline/"
        html_content = fetch_with_zyte(web)
        treatments = []
        processed_treatments = set()

        soup = BeautifulSoup(html_content, 'html.parser')
        slides = soup.select('.vi-slider__slide.vi-slider__slide--card')

        current_phase = None  # Initialize the current phase outside the loop

        for slide in slides:
            # Fetch the phase from preceding h2 tag if exists
            h2_tag = slide.find_previous("h2")
            if h2_tag:
                current_phase = h2_tag.text.strip()
            
            current_phase = clean_phase(current_phase)

            content_div = slide.select_one('.vi-pipeline-card__main')
            if content_div:
                # Extract text considering nested tags, properly handling <br> tags by converting them to line breaks
                full_text = ' '.join(content_div.h6.p.stripped_strings).replace('<br>', ' ')
                # Find the position of the last parenthesis that might enclose the scientific name or dosage form
                paren_pos = full_text.rfind(')') + 1

                if paren_pos > 0:
                    name = full_text[:paren_pos].strip()
                    indication = full_text[paren_pos:].strip() if len(full_text) > paren_pos else "Not Specified"
                else:
                    # No parentheses found, check for <br> tags logic or split on keywords
                    parts = full_text.split(' ')
                    name = parts[0]
                    indication = ' '.join(parts[1:]) if len(parts) > 1 else "Not Specified"

                # Post-process to remove specific keywords from name and treat as indication
                keywords = ["Immunology", "Neuroscience", "Gastrointestinal", "Oncology", "Asthma",
                            "Ulcerative Colitis", "Crohn’s Disease", "Multiple System Atrophy"]
                for keyword in keywords:
                    if keyword in name:
                        # Split on keyword and adjust name and indication
                        name_parts = name.split(keyword)
                        name = name_parts[0].strip()
                        indication = (keyword + ' ' + (' '.join(name_parts[1:]) + ' ' + indication).strip()).strip()

                # Clean up the extracted name and indication
                name = clean_text(name)
                indication = clean_text(indication)

                identification_key = generate_identification_key("Teva Pharmaceutical Industries", name, indication)
                date_scraped = datetime.now(timezone.utc)


                if "0" in indication:
                    indication = "Not Specified"

                if "284" in name:
                    name = "TEV-'284 / TEV-'294"

                if "ICS" in name:
                    indication = "Asthma"

                if "Anti-TL1A" in name:
                    indication = "Ulcerative Colitis Crohn’s Disease"

                if "Emrusolmin" in name:
                    indication = "Multiple System Atrophy"

                treatment_key = (name, indication, current_phase)
                
                indication_translator = MultilingualData()
                indication_translator.add_translation("en", indication.strip())

                indication_collection = MultilingualDataCollection()
                indication_collection.add_data(indication_translator)

                
                if treatment_key not in processed_treatments:
                    master_record = MasterTable(
                        company_name="Teva Pharmaceutical Industries",
                        treatment_name=name.strip(),
                        indication=indication_collection.get_collection_as_json(),
                        phase=current_phase,
                        identification_key=identification_key,
                        date_scraped=date_scraped
                    )
                    treatments.append(master_record.__dict__)  # Append the dictionary representation
                    processed_treatments.add(treatment_key)

        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping Teva Pharmaceutical's pipeline", e)
        return []

def astrazeneca_pipeline():
    try:
        web = "https://www.astrazeneca.com/our-therapy-areas/pipeline.html"
        html_content = fetch_with_zyte(web)
        treatments = []
        processed_treatments = set()
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extracting data
        areas = soup.select("section.pipeline__areas-region")
        for area in areas:
            area_name_full = area.find("h2", class_="pipeline__areas-title").text.strip()
            if '(' in area_name_full:
                area_name, last_updated = area_name_full.split(' (')
                last_updated = last_updated.strip(')')  # Clean up trailing parenthesis
            else:
                area_name = area_name_full
                last_updated = "Not specified"

            phases = area.select("div.pipeline__phases")
            for phase in phases:
                phase_name = phase.find("h3", class_="pipeline__phase-title").text.strip()

                compounds = phase.select("li.pipeline__compound")
                for compound in compounds:
                    compound_name = compound.find("strong", class_="pipeline__compound-name").text.strip()
                    description = compound.find("em", class_="pipeline__compound-description").text.strip()

                    # Fetch the popup for more details if it exists
                    details = compound.find("div", class_="pipeline__compound-popup")
                    mechanism = "Not specified"
                    date_commenced = "Not available"
                    molecule_size = "Not specified"
                    if details:
                        detail_list = details.select("li.pipeline__compound-detail")
                        for detail in detail_list:
                            if 'Mechanism:' in detail.text:
                                mechanism = detail.text.split('Mechanism: ')[1].strip()
                            if 'Date commenced phase:' in detail.text:
                                date_commenced = detail.text.split('Date commenced phase: ')[1].strip()
                            if 'Molecule size:' in detail.text:
                                molecule_size = detail.text.split('Molecule size: ')[1].strip()

                    area_name = clean_text(area_name)
                    last_updated = clean_text(last_updated)
                    phase_name = clean_phase(phase_name)
                    compound_name = clean_text(compound_name)
                    description = clean_text(description)
                    mechanism = clean_text(mechanism)
                    date_commenced = clean_text(date_commenced)
                    molecule_size = clean_text(molecule_size)
                    
                    identification_key = generate_identification_key("AstraZeneca", compound_name, description)
                    date_scraped = datetime.now(timezone.utc)          
                    
                    treatment_key = (compound_name, description, phase_name)


                    area_translator = MultilingualData()
                    description_translator = MultilingualData()
                    mechanism_translator = MultilingualData()
                    molecule_size_translator = MultilingualData()
                    
                    area_translator.add_translation("en", area_name)
                    description_translator.add_translation("en", description)
                    mechanism_translator.add_translation("en", mechanism)
                    molecule_size_translator.add_translation("en", molecule_size)

                    area_collection = MultilingualDataCollection()
                    description_collection = MultilingualDataCollection()
                    mechanism_collection = MultilingualDataCollection()
                    molecule_size_collection = MultilingualDataCollection()

                    area_collection.add_data(area_translator)
                    description_collection.add_data(description_translator)
                    mechanism_collection.add_data(mechanism_translator)
                    molecule_size_collection.add_data(molecule_size_collection)


                    if treatment_key not in processed_treatments:
                        master_record = MasterTable(
                            company_name="AstraZeneca",
                            phase=phase_name,
                            therapeutic_area=area_collection.get_collection_as_json(),
                            treatment_name=compound_name,
                            indication=description_collection.get_collection_as_json(),
                            target=mechanism_collection.get_collection_as_json(),
                            phase_commencement_date=date_commenced,
                            type_of_molecule=molecule_size,
                            date_last_changed=last_updated,
                            identification_key=identification_key,
                            date_scraped=date_scraped                       
                        )
                        treatments.append(master_record.__dict__)  # Append the dictionary representation
                        processed_treatments.add(treatment_key)
        return treatments, html_content
    except Exception as e:
        print("An error occurred scraping AstraZeneca's Pharmaceutical's pipeline", e)
        return []

def amgen_pipeline():
    try:
        web = "https://www.amgenpipeline.com/"
        html_content = fetch_with_zyte(web)
        treatments = []
        processed_treatments = set()
        soup = BeautifulSoup(html_content, 'html.parser')

        molecule_sections = soup.find_all('div', class_='row collapsibleContent')

        for section in molecule_sections:
            # Extract the molecule name
            molecule_name_container = section.find('div', class_='textContent')
            molecule_name = molecule_name_container.get_text(
                strip=True) if molecule_name_container else "Molecule name not available"

            # Extract the therapeutic area
            therapeutic_area_container = section.find('span', class_='tarea-text')
            therapeutic_area = therapeutic_area_container.get_text(
                strip=True) if therapeutic_area_container else "Therapeutic area not available"

            # Extract the investigational indication
            investigational_indication_container = section.find('div', class_='second-column')
            investigational_indication = investigational_indication_container.get_text(
                strip=True) if investigational_indication_container else "Indication not available"

            # Extract the modality
            modality_container = section.find('div', class_='third-column')
            modality = modality_container.get_text(strip=True) if modality_container else "Modality not available"

            # Extract the phase
            phase_container = section.find('span', class_='phases-PH3')
            phase = phase_container.get_text(strip=True) if phase_container else "Phase data not available"

            # Extracting the description
            description_container = section.find('p', class_='innterContentText')
            description = description_container.get_text(
                strip=True) if description_container else "Description not available"

            # Extracting additional information if available
            additional_info_elements = section.find_all('p', class_='innterContentText')
            additional_info = additional_info_elements[1].get_text(strip=True) if len(
                additional_info_elements) > 1 else "Additional information not available"
            
            molecule_name = clean_text(molecule_name)
            therapeutic_area = clean_text(therapeutic_area)
            investigational_indication = clean_text(investigational_indication)
            modality = clean_text(modality)
            phase = clean_phase(phase)
            description = clean_text(description)
            additional_info = clean_text(additional_info)
            
            identification_key = generate_identification_key("Amgen", molecule_name, description)
            date_scraped = datetime.now(timezone.utc)          
                    
            treatment_key = (molecule_name, description, phase)

            therapeutic_area_translator = MultilingualData()
            investigational_indication_translator = MultilingualData()
            modality_translator = MultilingualData()
            description_translator = MultilingualData()
            additional_info_translator = MultilingualData()

            therapeutic_area_translator.add_translation("en", therapeutic_area)
            investigational_indication_translator.add_translation("en", investigational_indication)
            modality_translator.add_translation("en", modality)
            description_translator.add_translation("en", description)
            additional_info_translator.add_translation("en", additional_info)


            therapeutic_area_collection = MultilingualDataCollection()
            investigational_indication_collection = MultilingualDataCollection()
            modality_collection = MultilingualDataCollection()
            description_collection = MultilingualDataCollection()
            additional_info_collection = MultilingualDataCollection()
            
            therapeutic_area_collection.add_data(therapeutic_area_translator)
            investigational_indication_collection.add_data(investigational_indication_translator)
            modality_collection.add_data(modality_translator)
            description_collection.add_data(description_translator)
            additional_info_collection.add_data(additional_info_translator)


            
            if treatment_key not in processed_treatments:
                master_record = MasterTable(
                    company_name="Amgen",
                    phase=phase,
                    therapeutic_area=therapeutic_area_collection.get_collection_as_json(),
                    treatment_name=molecule_name,
                    indication=investigational_indication_collection.get_collection_as_json(),
                    target=modality_collection.get_collection_as_json(),
                    type_of_molecule=description_collection.get_collection_as_json(),
                    notes=additional_info_collection.get_collection_as_json(),
                    identification_key=identification_key,
                    date_scraped=date_scraped                       
                )
                treatments.append(master_record.__dict__)  # Append the dictionary representation
                processed_treatments.add(treatment_key)
        return treatments, html_content
    except Exception as e:
            print("An error occurred scraping AstraZeneca's Pharmaceutical's pipeline", e)
            return []



# Function that cleans a string
def clean_text_merck(text):
    text = re.sub(r'[\n\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def parse_treatments_novartis(html):
    soup = BeautifulSoup(html, 'html.parser')
    treatments = []
    entries = soup.select(".pipeline-main-wrapper")
    for entry in entries:
        name = entry.select_one(".compound-name").text.strip()
        generic_name = entry.select_one(".generic-name").text.strip()
        indication = entry.select_one(".indication-name").text.strip()

        second_main = entry.select(".main-second span")

        # Initialize defaults
        therapeutic_area = "Null"
        phase = "Null"
        approval_year = "Null"
        mechanism_action = "Null"

        # Depending on number and content of spans, assign correctly
        if len(second_main) >= 3:
            therapeutic_area = second_main[0].text.strip()
            phase = clean_phase(second_main[1].text.strip())

            # Use regular expression to detect if it contains a year or range
            year_or_range = second_main[2].text.strip()
            if re.match(r"\d{4}", year_or_range) or "≥" in year_or_range:
                approval_year = year_or_range
                if len(second_main) > 3:
                    mechanism_action = second_main[3].text.strip()
            else:
                mechanism_action = second_main[2].text.strip()

        if len(second_main) <= 2:
            therapeutic_area = second_main[0].text.strip()
            phase = second_main[1].text.strip()

        treatments.append({
            "Company_Name": "Novartis",
            "Project": name,
            "Product": generic_name,
            "Indication": indication,
            "Therapeutic_Area": therapeutic_area,
            "Development_Phase": phase,
            "Filing_Date": approval_year,
            "Mechanism_of_Action": mechanism_action
        })

    return treatments


def generate_identification_key(company, treatment_name, indication):
    return f"{company}_{treatment_name}_{indication}".replace(" ", "_")


def table_insertion(treatments, html_content, company_name):
    # Connection parameters
    server = 'scrapedtreatmentsdatabase.database.windows.net'
    database = 'scrapedtreatmentssqldatabase'
    username = 'mzandi'
    password = 'Ranger22!'
    driver = '{ODBC Driver 18 for SQL Server}'

    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

    cursor = None
    connection = None

    # Get today's date for table name
    today = datetime.now(timezone.utc).strftime('%Y%m%d')


    try:
        # Connect to your Azure SQL Database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        for treatment in treatments:
            treatment_key = treatment['Identification_Key']

            # Format the treatment data for today
            new_data = {today: treatment}
            
            # Check if a record exists
            cursor.execute("SELECT Treatment_Data FROM Revised_MasterTable WHERE Treatment_Key = ?", (treatment_key,))
            result = cursor.fetchone()
            
            if result:
                # If exists, load the current data and append the new data
                existing_data = json.loads(result[0])
                existing_data.append(new_data)  # Append the new data to the list
                json_data = json.dumps(existing_data, default=str)
                cursor.execute("UPDATE Revised_MasterTable SET Treatment_Data = ? WHERE Treatment_Key = ?", (json_data, treatment_key))
            else:
                # If not exists, create a new list with the current treatment data
                json_data = json.dumps([new_data], default=str)  # Start with a list containing today's data
                cursor.execute("INSERT INTO Revised_MasterTable (Company_Name, Treatment_Key, Treatment_Data) VALUES (?, ?, ?)",
                            (treatment['Company_Name'], treatment_key, json_data))


        # Prepare to insert HTML content
        logging.info(
            f'Attempting to insert HTML content for {company_name}. Content size: {len(html_content.encode("utf-8"))} bytes.')
        insert_query_two = '''
         INSERT INTO WebPageContents (Company_Name, HTML_Content, Date_Retrieved)
         VALUES (?, ?, ?)
         '''

        # Check if HTML content is within size limit before inserting
        if len(html_content.encode('utf-8')) < 2000000000:  # Less than 2 GB
            date_html_analyzed = datetime.now(timezone.utc)
            cursor.execute(insert_query_two, (company_name, html_content, date_html_analyzed))
        else:
            logging.info(f"HTML content too large to store for {company_name}. Skipping insertion.")

        connection.commit()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def clear_remake_tables():
    server = 'scrapedtreatmentsdatabase.database.windows.net'
    database = 'scrapedtreatmentssqldatabase'
    username = 'mzandi'
    password = 'Ranger22!'
    driver = '{ODBC Driver 18 for SQL Server}'

    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'


    try:
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # Ensure the main table for treatments exists
                logging.info("Checking and creating Revised_MasterTable if necessary.")
                cursor.execute("""
                    IF OBJECT_ID('dbo.Revised_MasterTable', 'U') IS NULL
                    CREATE TABLE Revised_MasterTable (
                        Company_Name NVARCHAR(255),
                        Treatment_Key NVARCHAR(MAX),
                        Treatment_Data NVARCHAR(MAX)
                    );
                """)
                connection.commit()

                # Ensure the table for storing HTML content exists
                logging.info("Checking and creating WebPageContents if necessary.")
                cursor.execute("""
                    IF OBJECT_ID('dbo.WebPageContents', 'U') IS NULL
                    CREATE TABLE WebPageContents (
                        ID INT PRIMARY KEY IDENTITY(1,1),
                        Company_Name NVARCHAR(255),
                        HTML_Content NVARCHAR(MAX),
                        Date_Retrieved DATETIME
                    );
                """)
                connection.commit()

    except Exception as e:
        logging.error(f"An error occurred during database setup: {e}")
        logging.error(f"An error occurred: {e}")

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="HttpTrigger1")
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    
    logging.info('Python HTTP trigger function processed a request.')

    try:
        logging.info("Starting clear_remake_tables function.")
        clear_remake_tables()
        logging.info("Finished clear_remake_tables function.")
        
        # Set the current time to test
        current_time = datetime.now(timezone.utc)
        logging.info(f"Current time: {current_time}")

        # Database connection setup
        server = 'scrapedtreatmentsdatabase.database.windows.net'
        database = 'scrapedtreatmentssqldatabase'
        username = 'mzandi'
        password = 'Ranger22!'
        driver = '{ODBC Driver 18 for SQL Server}'
        connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        cursor.execute("SELECT Scraping_Objects FROM Calendar WHERE Time <= ?", (current_time,))
        result = cursor.fetchone()

        if result:
            scraping_objects = json.loads(result[0])
            logging.info(f"Scraping objects found: {scraping_objects}")
            
            for obj in scraping_objects:
                object_code = obj['objectCode']
                logging.info(f"Object Code: {object_code}")
                
                # Attempt to find the function by name
                func = globals().get(object_code)
                
                if func:
                    logging.info(f"Executing function for {object_code}")
                    # Call the scraping function
                    data, content = func()  # Assuming each scraping function returns a tuple (data, content)
                    
                    # Extract the first word from objectDescription for the table name
                    table_name = obj['objectDescription'].split()[0]
                    
                    # Call the table insertion function
                    logging.info(f'Inserting data for {table_name}...')
                    table_insertion(data, content, table_name)
                else:
                    logging.error(f"No function matched for {object_code}")
        else:
            logging.info("No matching objects found in the Calendar table.")
        
        logging.info("translate_trigger function called")
        translate_trigger()
        return json.dumps({"status": "success", "message": "All data processed and inserted successfully."})

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return func.HttpResponse(
            f"Error processing data: {e}",
            status_code=500
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()




def translate_trigger():
    server = 'scrapedtreatmentsdatabase.database.windows.net'
    database = 'scrapedtreatmentssqldatabase'
    username = 'mzandi'
    password = 'Ranger22!'
    driver = '{ODBC Driver 18 for SQL Server}'

    database_connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

    # This example simply processes all entries, or you could add a condition based on another column
    query = "SELECT Treatment_Key, Treatment_Data FROM Revised_MasterTable"

    try:
        with pyodbc.connect(database_connection_string) as conn:
            cursor = conn.cursor()
            logging.info("Connected to the database")

            cursor.execute(query)
            rows = cursor.fetchall()
            logging.info(f"Fetched {len(rows)} rows for processing")

            for row in rows:
                treatment_key, treatment_data_json = row
                try:
                    treatment_data = json.loads(treatment_data_json)
                    # Process and translate row
                    process_and_translate_row(treatment_data, cursor, treatment_key)
                    updated_json = json.dumps(treatment_data)  # Ensure JSON serialization
                    cursor.execute("UPDATE Revised_MasterTable SET Treatment_Data = ? WHERE Treatment_Key = ?", (updated_json, treatment_key))
                except Exception as e:
                    logging.error(f"Error processing treatment with key {treatment_key}: {e}")

            conn.commit()
            logging.info("Translation processing completed and database updated")

            try:
                second_endpoint_url = "http://127.0.0.1:5000/trigger-notifications"
                response = requests.get(second_endpoint_url)
                response.raise_for_status()  # Raises an HTTPError if the response code was unsuccessful
                logging.info("Second endpoint triggered successfully.")
            except requests.exceptions.RequestException as e:
                logging.error(f"Error calling the second endpoint: {e}")

        return func.HttpResponse(
            "Translation processing completed.",
            status_code=200
        )

    except pyodbc.Error as e:
        logging.error(f"Database error: {e}")
        return func.HttpResponse(
            f"Database error: {e}",
            status_code=500
        )
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse(
            f"Unexpected error: {e}",
            status_code=500
        )

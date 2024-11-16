# Zanalytix Pharma Pipeline Scraper - Back-End

## Overview

The back-end of the **Zanalytix Pharma Pipeline Scraper** is responsible for web scraping, data processing, storage, and serving APIs for the front-end. It manages pharmaceutical pipeline data, automates updates, and ensures seamless communication with the front-end while supporting robust authentication and error handling.

## Features

- **Automated Web Scraping:** Scrapes pharmaceutical company websites regularly for pipeline updates.
- **Data Storage and Management:** Stores pipeline data in an Azure SQL database with efficient querying capabilities.
- **RESTful API Services:** Provides endpoints for accessing and managing pipeline data.
- **Notification System:** Enables real-time notifications based on user-defined preferences.
- **Error Logging and Monitoring:** Ensures reliable performance and debugging with robust logging.

## Technologies Used

- **Framework:** Flask for building lightweight and scalable back-end services.
- **Web Scraping Tools:** Zyte API and BeautifulSoup for extracting data from pharmaceutical websites.
- **Database:** Azure SQL Server for secure and scalable data storage.
- **Authentication:** JSON Web Tokens (JWT) for secure API authentication.
- **Deployment:** Azure Functions for scheduling and automating scraping tasks.
- **AI and Automation:**
  - **LangChain:** Utilized for integrating large language models (LLMs) to process and analyze extracted data.
  - **LLMs (e.g., OpenAI GPT):** Enhance data extraction and processing for intelligent insights.
  - **Master Scheduler:** Custom Python logic to automate and manage scraping schedules dynamically.
- **Data Cleaning:** Python modules (`cleanup_phase.py` and `cleanup_text.py`) for preprocessing scraped data to ensure consistency and accuracy.
- **Error Handling and Logging:** Python logging module for debugging and error tracking.
- **API Documentation:** Flask-RESTful and Swagger (if applicable) for documenting and testing APIs.
- **Contact Preferences Management:** Files like `contact_preferences.py` and `fetch_preference_options.py` handle user-configured notifications.
- **Visualization:** `treatment_visualizer.py` for generating user-friendly visualizations of pipeline data.
- **User Management:** Endpoints like `login.py`, `sign_up.py`, and `forgot_password_endpoints.py` for secure user authentication and account recovery.

## Back-End Interaction

- **GET /pipelines:** Retrieve all pipeline data.
- **POST /notifications:** Add or update notification preferences.
- **PUT /scraping-objects:** Update scraping configurations.
- **DELETE /pipelines/{id}:** Remove a pipeline entry.

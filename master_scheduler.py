import pyodbc
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from dateutil.rrule import rrulestr, rrule
from datetime import datetime
from notifications import notifications_bp  # Import the Blueprint
from contact_preferences import contact_preferences_bp
from fetch_preference_options import preferences_bp  # Import the blueprint
from scheduling_endpoints import scheduling_options_bp  # Import the blueprint
from sign_up import signup_bp  # Import the auth blueprint
from login import login_bp  # Import the auth blueprint
from retreive_all_preferences import retreive_options_bp  # Import the blueprint
from admin_endpoints import admin_console_bp  # Import the blueprint
from treatment_visualizer import fetch_treatments_bp
from forgot_password_endpoints import forgot_password_bp

app = Flask(__name__)
CORS(app)

app.register_blueprint(notifications_bp)
app.register_blueprint(contact_preferences_bp)
app.register_blueprint(preferences_bp)
app.register_blueprint(scheduling_options_bp)
app.register_blueprint(signup_bp)
app.register_blueprint(login_bp)
app.register_blueprint(retreive_options_bp)
app.register_blueprint(admin_console_bp)
app.register_blueprint(fetch_treatments_bp)
app.register_blueprint(forgot_password_bp)



if __name__ == '__main__':
    app.run(debug=True, port=5000)
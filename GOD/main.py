import pandas as pd
import numpy as np
import time
from datetime import datetime,date
from openpyxl import Workbook, load_workbook
from fyers_apiv3.FyersWebsocket import data_ws
from openpyxl.styles import PatternFill
import credentials as c
from fyers import FyersLiveData, create_fyers_session, create_xlsx_file, store_ltp_data
import logging
import os

SYMBOL= "NSE:NIFTY50-INDEX"
# Initialize variables for LTP collection
TIMEFRAME = 1  # in minutes (can be modified: 1, 3, 5, 15, etc.)
DATA_POINTS = TIMEFRAME * 60  # Number of seconds in the timeframe
COLLECTION_INTERVAL = 1  # Collect data every second


# Ensure Log directory exists
log_dir = "Log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Set up logging to Log/live.log
logging.basicConfig(
    filename=os.path.join(log_dir, 'live.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

logging.info("Script started.")

try:
    # Read the URL from the file
    logging.info("Attempting to read auth URL from URL.txt")
    with open("URL.txt", "r") as file:
        url = file.read().strip()
        logging.info("Successfully read URL from file")
except FileNotFoundError:
    logging.error("URL.txt file not found")
    raise
except Exception as e:
    logging.error(f"Error reading URL file: {str(e)}")
    raise

# Extract the auth code from the URL
try:
    s1 = url.split('auth_code=')
    authcode = s1[1].split('&state')[0]
    logging.info("Successfully extracted auth code from URL")
except IndexError:
    logging.error("Failed to extract auth code from URL - Invalid URL format")
    raise
except Exception as e:
    logging.error(f"Error extracting auth code: {str(e)}")
    raise


logging.info(f"Trading symbol set to: {SYMBOL}")


# Define your Fyers API credentials
client_id = c.client_id
secret_key = c.secret_key
redirect_uri = c.redirect_uri # Replace with your redirect URI
response_type = c.response_type 
grant_type = c.grant_type  


# Create a session model with the provided credentials
try:
    logging.info("Attempting to create Fyers session")
    fyers_main, access_token = create_fyers_session(authcode)
    logging.info("Successfully created Fyers session")
    
    user_profile = fyers_main.get_profile()
    logging.info("Successfully retrieved user profile")
except Exception as e:
    logging.error(f"Failed to create Fyers session or get profile: {str(e)}")
    raise



try:
    file_path = create_xlsx_file(SYMBOL)
    logging.info(f"Successfully created Excel file at: {file_path}")
except Exception as e:
    logging.error(f"Error creating Excel file: {str(e)}")
    raise


ltp_list = []

try:
    live_data = FyersLiveData(fyers_main, SYMBOL)
    logging.info(f"Starting LTP data collection for {TIMEFRAME} minute timeframe")
    
    while True:
        current_time = datetime.now()
        
        # Wait until the start of the next minute
        if current_time.second != 0:
            wait_seconds = 60 - current_time.second
            logging.info(f"Waiting {wait_seconds} seconds for next minute to start")
            time.sleep(wait_seconds)
        
        # Only start at the beginning of a timeframe
        if current_time.minute % TIMEFRAME != 0:
            continue
            
        # Clear the list for new timeframe's data
        ltp_list.clear()
        logging.info(f"Starting data collection at {datetime.now().strftime('%H:%M:%S')} for {TIMEFRAME} minute(s)")
        
        # Collect LTP data for the specified timeframe
        start_time = time.time()
        for _ in range(DATA_POINTS):
            try:
                if time.time() - start_time >= TIMEFRAME * 60:  # Ensure we don't overflow into next timeframe
                    break
                    
                ltp = live_data.get_ltp()
                ltp_list.append(ltp)
                logging.debug(f"LTP collected: {ltp}")
                
                # Calculate sleep time to maintain collection interval
                elapsed = time.time() - start_time
                next_second = int(elapsed) + COLLECTION_INTERVAL
                sleep_time = next_second - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            except Exception as e:
                logging.warning(f"Failed to collect LTP: {str(e)}")
                continue
        
        # Store the collected data
        if ltp_list:
            logging.info(f"Collected {len(ltp_list)} LTP points for {TIMEFRAME} minute timeframe")
            store_ltp_data(ltp_list, file_path)
            logging.info("Successfully stored LTP data in Excel")
        else:
            logging.error("No LTP data collected")
            
except Exception as e:
    logging.error(f"Error in live data processing: {str(e)}")
    raise
except KeyboardInterrupt:
    logging.info("Data collection stopped by user")
    raise


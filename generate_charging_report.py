"""
MIT License

Copyright (c) 2024 Julian Bartholomeyczik

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

"""
Purpose:
This program integrates data from Wallbox and Tibber APIs to provide a comprehensive overview of electric vehicle charging sessions and energy consumption. It fetches data from both APIs, processes it, and caches the results to minimize redundant API calls. The data is then used to generate reports in various formats such as PDF and Excel.

Features:
- Fetches and processes data from Wallbox and Tibber APIs.
- Caches data to reduce API call frequency.
- Converts and processes datetime information.
- Generates reports in PDF and Excel formats.

Usage:
1. Ensure you have the required API keys for Wallbox and Tibber.
2. Store the API keys in a separate `secrets.py` file.
3. Run the script to fetch, process, and generate reports based on the data.
"""

import requests
import pandas as pd
import pickle
import os
import logging
from datetime import datetime, timedelta
from fpdf import FPDF
import xlsxwriter
from secrets_1 import TIBBER_API_KEY, WALLBOX_API_KEY  # Import the API keys from secrets_1.py

CACHE_FILE = "cache.pkl"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_cache_valid():
    """Check if cache is valid (within the last hour)."""
    if not os.path.exists(CACHE_FILE):
        return False

    with open(CACHE_FILE, 'rb') as cache_file:
        cache = pickle.load(cache_file)
    
    last_fetch_time = cache['timestamp']
    if datetime.now() - last_fetch_time < timedelta(hours=1):
        return True
    return False

def load_cache():
    """Load the cached data using pickle."""
    with open(CACHE_FILE, 'rb') as cache_file:
        cache = pickle.load(cache_file)
    
    wallbox_data = cache['wallbox_data']
    tibber_df = cache['tibber_data']
    address = cache['address']
    owner = cache['owner']
    
    # Ensure datetime conversion
    wallbox_data['start'] = pd.to_datetime(wallbox_data['start'])
    wallbox_data['end'] = pd.to_datetime(wallbox_data['end'])
    tibber_df['from'] = pd.to_datetime(tibber_df['from'], utc=True).dt.tz_localize(None)
    tibber_df['to'] = pd.to_datetime(tibber_df['to'], utc=True).dt.tz_localize(None)

    return wallbox_data, tibber_df, address, owner

def save_cache(wallbox_data, tibber_df, address, owner):
    """Save the fetched data to cache using pickle."""
    cache = {
        'timestamp': datetime.now(),
        'wallbox_data': wallbox_data,
        'tibber_data': tibber_df,
        'address': address,
        'owner': owner
    }
    
    with open(CACHE_FILE, 'wb') as cache_file:
        pickle.dump(cache, cache_file)

def fetch_wallbox_data():
    """Fetch data from Wallbox API."""
    try:
        url = WALLBOX_API_KEY
        response = requests.get(url)
        response.raise_for_status()
        wallbox_json_data = response.json()

        columns = [col['key'] for col in wallbox_json_data['columns']]
        data = wallbox_json_data['data']

        wallbox_data = pd.DataFrame(data, columns=columns)
        wallbox_data['start'] = pd.to_datetime(wallbox_data['start'], format='%d.%m.%Y %H:%M:%S')
        wallbox_data['end'] = pd.to_datetime(wallbox_data['end'], format='%d.%m.%Y %H:%M:%S')
        wallbox_data['energy'] = wallbox_data['energy'].astype(float)

        address = wallbox_json_data.get('address', {'address1': 'Unknown', 'postalCode': '00000', 'city': 'Unknown'})
        owner = wallbox_json_data.get('owner', {'firstName': 'Unknown', 'lastName': 'Unknown'})
        
        return wallbox_data, address, owner
    except requests.RequestException as e:
        logging.error(f"Error fetching Wallbox data: {e}")
        return None, None, None

def fetch_tibber_data():
    """Fetch data from Tibber API using pagination to overcome API limits."""
    try:
        tibber_url = "https://api.tibber.com/v1-beta/gql"
        headers = {
            "Authorization": f"Bearer {TIBBER_API_KEY}",
            "Content-Type": "application/json"
        }

        all_data = []
        has_next_page = True
        cursor = None

        while has_next_page:
            query = """
            query ($after: String) {
              viewer {
                homes {
                  address {
                    address1
                    postalCode
                    city
                  }
                  owner {
                    firstName
                    lastName
                  }
                  consumption(resolution: HOURLY, first: 744, after: $after) {
                    pageInfo {
                      hasNextPage
                      endCursor
                    }
                    nodes {
                      from
                      to
                      cost
                      unitPrice
                      consumption
                    }
                  }
                }
              }
            }
            """

            variables = {"after": cursor}
            response = requests.post(tibber_url, headers=headers, json={"query": query, "variables": variables})
            response.raise_for_status()
            tibber_json_data = response.json()

            home_data = tibber_json_data['data']['viewer']['homes'][0]
            consumption_data = home_data['consumption']
            all_data.extend(consumption_data['nodes'])

            # Update pagination info
            has_next_page = consumption_data['pageInfo']['hasNextPage']
            cursor = consumption_data['pageInfo']['endCursor']

        # Create DataFrame from all collected data
        tibber_df = pd.DataFrame(all_data)
        tibber_df['from'] = pd.to_datetime(tibber_df['from'], utc=True).dt.tz_localize(None)
        tibber_df['to'] = pd.to_datetime(tibber_df['to'], utc=True).dt.tz_localize(None)

        address = home_data.get('address', {'address1': 'Unknown', 'postalCode': '00000', 'city': 'Unknown'})
        owner = home_data.get('owner', {'firstName': 'Unknown', 'lastName': 'Unknown'})

        return tibber_df, address, owner
    except requests.RequestException as e:
        logging.error(f"Error fetching Tibber data: {e}")
        return None, None, None


def calculate_tibber_price(start_time, end_time, tibber_df):
    price_during_session = tibber_df[(tibber_df['from'] <= end_time) & (tibber_df['to'] >= start_time)]
    if (price_during_session.empty):
        print(f"No matching Tibber price found for session between {start_time} and {end_time}")
        return 0  # Return 0 when no match is found
    return price_during_session['unitPrice'].mean()

# Export the Wallbox data with Tibber prices to CSV
def export_table_with_prices(wallbox_data, tibber_df):
    wallbox_data['Tibber Price [EUR/kWh]'] = wallbox_data.apply(
        lambda row: calculate_tibber_price(row['start'], row['end'], tibber_df), axis=1
    )
    wallbox_data['Total Cost [EUR]'] = wallbox_data['energy'] * wallbox_data['Tibber Price [EUR/kWh]']
    wallbox_data[['energy', 'Tibber Price [EUR/kWh]', 'Total Cost [EUR]']] = wallbox_data[['energy', 'Tibber Price [EUR/kWh]', 'Total Cost [EUR]']].round(2)
    wallbox_data.to_csv('updated_wallbox_with_tibber_prices.csv', index=False)
    print("CSV export completed: 'updated_wallbox_with_tibber_prices.csv'")

# Generate Excel report
def generate_excel_report(filtered_data, start_date, end_date, total_energy, total_cost, address, owner):
    # Create an Excel workbook and sheet
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    workbook = xlsxwriter.Workbook(f'charging_report_{timestamp}.xlsx')
    worksheet = workbook.add_worksheet('Charging Report')

    # Set column width for better readability
    worksheet.set_column(0, 3, 20)

    # Define some formats
    title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center'})
    header_format = workbook.add_format({'bold': True, 'bg_color': '#F9DA04', 'align': 'center', 'border': 1})
    cell_format = workbook.add_format({'align': 'center', 'border': 1})
    summary_format = workbook.add_format({'bold': True, 'align': 'left', 'border': 1})

    # Title of the report
    worksheet.merge_range('A1:D1', 'Charging Report', title_format)

    # Add summary information
    worksheet.write('A3', 'Start Date:', summary_format)
    worksheet.write('B3', str(start_date), summary_format)
    worksheet.write('A4', 'End Date:', summary_format)
    worksheet.write('B4', str(end_date), summary_format)
    worksheet.write('A5', 'Total Energy Consumed (kWh):', summary_format)
    worksheet.write('B5', f"{total_energy:.2f} kWh", summary_format)
    worksheet.write('A6', 'Total Cost (EUR):', summary_format)
    worksheet.write('B6', f"{total_cost:.2f} EUR", summary_format)

    # Address and owner details
    worksheet.write('A8', 'Address:', summary_format)
    worksheet.write('B8', f"{address['address1']}, {address['postalCode']} {address['city']}", summary_format)
    worksheet.write('A9', 'Owner:', summary_format)
    worksheet.write('B9', f"{owner['firstName']} {owner['lastName']}", summary_format)

    # Add table headers for charging sessions
    worksheet.write('A11', 'Date', header_format)
    worksheet.write('B11', 'Energy (kWh)', header_format)
    worksheet.write('C11', 'Cost per kWh (EUR)', header_format)
    worksheet.write('D11', 'Total Cost (EUR)', header_format)

    # Write the charging session data
    row = 11
    for _, session in filtered_data.iterrows():
        worksheet.write(row, 0, str(session['start']), cell_format)
        worksheet.write(row, 1, f"{session['energy']:.2f}", cell_format)
        worksheet.write(row, 2, f"{session['Tibber Price [EUR/kWh]']:.2f}", cell_format)
        worksheet.write(row, 3, f"{session['Total Cost [EUR]']:.2f}", cell_format)
        row += 1

    # Close the workbook
    workbook.close()
    print("Excel report generated: 'charging_report.xlsx'")

def generate_pdf_report(filtered_data, start_date, end_date, total_energy, total_cost, address, owner):
    pdf = FPDF()
    pdf.add_page()

    # Title
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(0, 0, 128)  # Dark blue title color
    pdf.cell(200, 10, 'Charging Report', ln=True, align='C')

    # Set the body font and text color back to black
    pdf.set_font('Arial', '', 12)
    pdf.set_text_color(0, 0, 0)

    # Summary details
    pdf.ln(10)  # Add some space
    pdf.cell(50, 10, 'Start Date:', ln=False)
    pdf.cell(100, 10, str(start_date), ln=True)
    
    pdf.cell(50, 10, 'End Date:', ln=False)
    pdf.cell(100, 10, str(end_date), ln=True)

    pdf.cell(50, 10, 'Total Energy Consumed:', ln=False)
    pdf.cell(100, 10, f"{total_energy:.2f} kWh", ln=True)

    pdf.cell(50, 10, 'Total Cost:', ln=False)
    pdf.cell(100, 10, f"{total_cost:.2f} EUR", ln=True)

    pdf.ln(5)
    pdf.cell(50, 10, 'Address:', ln=False)
    pdf.cell(100, 10, f"{address['address1']}, {address['postalCode']} {address['city']}", ln=True)
    
    pdf.cell(50, 10, 'Owner:', ln=False)
    pdf.cell(100, 10, f"{owner['firstName']} {owner['lastName']}", ln=True)

    # Add table headers
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 12)
    pdf.set_fill_color(220, 220, 220)  # Light gray for header background
    pdf.cell(50, 10, 'Date', border=1, align='C', fill=True)
    pdf.cell(40, 10, 'Energy (kWh)', border=1, align='C', fill=True)
    pdf.cell(50, 10, 'Tibber Price [EUR/kWh]', border=1, align='C', fill=True)
    pdf.cell(50, 10, 'Total Cost (EUR)', border=1, align='C', fill=True)
    pdf.ln()

    # Add rows for each charging session
    pdf.set_font('Arial', '', 12)
    for _, session in filtered_data.iterrows():
        pdf.cell(50, 10, str(session['start']), border=1, align='C')
        pdf.cell(40, 10, f"{session['energy']:.2f}", border=1, align='C')
        pdf.cell(50, 10, f"{session['Tibber Price [EUR/kWh]']:.4f}", border=1, align='C')
        pdf.cell(50, 10, f"{session['Total Cost [EUR]']:.2f}", border=1, align='C')
        pdf.ln()

    # Add the date and time to the filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pdf_output_filename = f'charging_report_{timestamp}.pdf'
    
    # Save the PDF
    pdf.output(pdf_output_filename)

    print(f"PDF report generated: {pdf_output_filename}")

# Process data and calculate total consumption and cost
def process_data_with_filters(wallbox_data, tibber_df, id_chip_name, start_date, end_date, address, owner):
    filtered_data = wallbox_data[
        (wallbox_data['id_chip_name'].str.contains(id_chip_name, case=False, na=False)) &
        (wallbox_data['start'] >= start_date) &
        (wallbox_data['end'] <= end_date)
    ]

    print(f"Number of rows in filtered_data before applying Tibber prices: {len(filtered_data)}")
# Fix for 'SettingWithCopyWarning'
    filtered_data.loc[:, 'Tibber Price [EUR/kWh]'] = filtered_data.apply(
        lambda row: calculate_tibber_price(row['start'], row['end'], tibber_df), axis=1
    )
    filtered_data.loc[:, 'Total Cost [EUR]'] = filtered_data['energy'] * filtered_data['Tibber Price [EUR/kWh]']

    print(f"Number of rows in Tibber Price column after apply: {len(filtered_data['Tibber Price [EUR/kWh]'])}")

    filtered_data['Total Cost [EUR]'] = filtered_data['energy'] * filtered_data['Tibber Price [EUR/kWh]']
    total_energy = filtered_data['energy'].sum()
    total_cost = filtered_data['Total Cost [EUR]'].sum()

    print(f"Total Energy Consumed: {total_energy:.2f} kWh")
    print(f"Total Cost: {total_cost:.2f} EUR")

    generate_excel_report(filtered_data, start_date, end_date, total_energy, total_cost, address, owner)
    generate_pdf_report(filtered_data, start_date, end_date, total_energy, total_cost, address, owner)

    print("Excel and PDF reports have been generated.")

# Process data and calculate total consumption and cost for the given range and ID chip name
def main():
    # Step 1: Check if cache is valid (within the last hour)
    if is_cache_valid():
        print("Using cached data...")
        wallbox_data, tibber_df, address, owner = load_cache()
    else:
        print("Fetching new Wallbox and Tibber data...")
        wallbox_data, address, owner = fetch_wallbox_data()
        tibber_df, tibber_address, tibber_owner = fetch_tibber_data()

        # Cache the fetched data
        save_cache(wallbox_data, tibber_df, tibber_address, tibber_owner)
        
        # Use Tibber address and owner details
        address = tibber_address
        owner = tibber_owner

    # Step 2: Export Wallbox data to CSV with Tibber prices
    print("Exporting Wallbox data with Tibber prices to CSV...")
    export_table_with_prices(wallbox_data, tibber_df)

    # Step 3: Prompt user for input (chip name, start and end dates)
    id_chip_name = input("Enter the ID chip name (e.g., 'Volvo'): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    # Convert input strings to datetime objects
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    # Step 4: Process data for filtered results and generate reports using Tibber address and owner info
    print(f"Processing data for {id_chip_name} from {start_date} to {end_date}...")
    process_data_with_filters(wallbox_data, tibber_df, id_chip_name, start_date, end_date, address, owner)

    print("Reports generated successfully (CSV, Excel, PDF).")

# Make sure this runs when the script is executed
if __name__ == "__main__":
    main()

import csv
import sqlite3
import xml.etree.ElementTree as ET
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError
from pathlib import Path
import time
import logging
import re
from pypdf import PdfReader

# Configuration flags to control which files to process
PROCESS_CSV = False
PROCESS_TAB = False
PROCESS_XML = False
PROCESS_YAML = False
PROCESS_PDF = True

# Configuration flags to clear tables before parsing
CLEAR_CSV = False
CLEAR_TAB = False
CLEAR_XML = False
CLEAR_YAML = False
CLEAR_PDF = True


# File paths
CSV_FILE = 'Data/BoardingData.csv'
TAB_FILE = 'Data/Sirena-export-fixed.tab'
XML_FILE = 'Data/PointzAggregator-AirlinesData.xml'
YAML_FILE = 'Data/SkyTeam-Exchange.yaml'
PDF_FILE = 'Data/Skyteam_Timetable.pdf'
DB_FILE = 'xx.db'

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_connection(db_file: str) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Create a connection to the SQLite database and return the connection and cursor."""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        return conn, cursor
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def create_boarding_data_table(cursor: sqlite3.Cursor) -> None:
    """Create the boarding_data table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS boarding_data (
            PassengerFirstName TEXT,
            PassengerSecondName TEXT,
            PassengerLastName TEXT,
            PassengerSex TEXT,
            PassengerBirthDate TEXT,
            PassengerDocument TEXT,
            BookingCode TEXT,
            TicketNumber TEXT,
            Baggage TEXT,
            FlightDate TEXT,
            FlightTime TEXT,
            FlightNumber TEXT,
            CodeShare TEXT,
            Destination TEXT
        )
    ''')

def create_sirena_data_table(cursor: sqlite3.Cursor) -> None:
    """Create the sirena_data table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sirena_data (
            PaxName TEXT,
            PaxBirthDate TEXT,
            DepartDate TEXT,
            DepartTime TEXT,
            ArrivalDate TEXT,
            ArrivalTime TEXT,
            FlightCode TEXT,
            FromAirport TEXT,
            Dest TEXT,
            Code TEXT,
            e_Ticket TEXT,
            TravelDoc TEXT,
            Seat TEXT,
            Meal TEXT,
            TrvCls TEXT,
            Fare TEXT,
            Baggage TEXT,
            PaxAdditionalInfo TEXT,
            AgentInfo TEXT
        )
    ''')

def create_pointz_aggregator_table(cursor: sqlite3.Cursor) -> None:
    """Create the pointz_aggregator_data table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pointz_aggregator_data (
            UserUID TEXT,
            FirstName TEXT,
            LastName TEXT,
            CardNumber TEXT,
            BonusProgramm TEXT,
            FlightCode TEXT,
            FlightDate TEXT,
            Departure TEXT,
            Arrival TEXT,
            Fare TEXT
        )
    ''')

def create_skyteam_data_table(cursor: sqlite3.Cursor) -> None:
    """Create the skyteam_data table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS skyteam_data (
            FlightDate TEXT,
            FlightNumber TEXT,
            FFProgram TEXT,
            FFNumber TEXT,
            TravelClass TEXT,
            Fare TEXT,
            Departure TEXT,
            Arrival TEXT,
            Status TEXT
        )
    ''')

def create_skyteam_timetable_table(cursor: sqlite3.Cursor) -> None:
    """Create the skyteam_timetable table if it doesn't exist."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS skyteam_timetable (
            FromCity TEXT,
            FromCountry TEXT,
            FromAirport TEXT,
            ToCity TEXT,
            ToCountry TEXT,
            ToAirport TEXT,
            Validity TEXT,
            Days TEXT,
            DepTime TEXT,
            ArrTime TEXT,
            Flight TEXT,
            Aircraft TEXT,
            TravelTime TEXT
        )
    ''')

def parse_csv_file(cursor: sqlite3.Cursor, csv_file: str) -> None:
    """Parse the CSV file and insert data into the boarding_data table."""
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            next(reader)  # Skip header
            for row in reader:
                if len(row) == 14:  # Validate row length
                    cursor.execute('''
                        INSERT INTO boarding_data (
                            PassengerFirstName, PassengerSecondName, PassengerLastName, PassengerSex,
                            PassengerBirthDate, PassengerDocument, BookingCode, TicketNumber,
                            Baggage, FlightDate, FlightTime, FlightNumber, CodeShare, Destination
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', row)
                else:
                    logger.warning(f"Skipping invalid CSV row: {row}")
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        raise
    except Exception as e:
        logger.error(f"Error parsing CSV file: {e}")
        raise

def parse_tab_file(cursor: sqlite3.Cursor, tab_file: str) -> None:
    """Parse the TAB file and insert data into the sirena_data table."""
    try:
        with open(tab_file, 'r', encoding='utf-8') as file:
            next(file)  # Skip header
            for line in file:
                line = line.rstrip()
                if not line:
                    continue
                try:
                    row = [
                        line[0:60].strip(),   # PaxName
                        line[60:72].strip(),  # PaxBirthDate
                        line[72:84].strip(),  # DepartDate
                        line[84:96].strip(),  # DepartTime
                        line[96:108].strip(), # ArrivalDate
                        line[108:120].strip(),# ArrivalTime
                        line[120:132].strip(),# FlightCode
                        line[132:138].strip(),# FromAirport
                        line[138:144].strip(),# Dest
                        line[144:150].strip(),# Code
                        line[150:168].strip(),# e_Ticket
                        line[168:180].strip(),# TravelDoc
                        line[180:186].strip(),# Seat
                        line[186:192].strip(),# Meal
                        line[192:198].strip(),# TrvCls
                        line[198:216].strip(),# Fare
                        line[216:240].strip(),# Baggage
                        line[240:276].strip(),# PaxAdditionalInfo
                        line[276:336].strip() # AgentInfo
                    ]
                    if len(row) == 19:
                        cursor.execute('''
                            INSERT INTO sirena_data (
                                PaxName, PaxBirthDate, DepartDate, DepartTime, ArrivalDate,
                                ArrivalTime, FlightCode, FromAirport, Dest, Code,
                                e_Ticket, TravelDoc, Seat, Meal, TrvCls,
                                Fare, Baggage, PaxAdditionalInfo, AgentInfo
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                    else:
                        logger.warning(f"Skipping invalid TAB row: {row}")
                except IndexError:
                    logger.warning(f"Skipping malformed TAB line: {line}")
    except FileNotFoundError:
        logger.error(f"TAB file not found: {tab_file}")
        raise
    except Exception as e:
        logger.error(f"Error parsing TAB file: {e}")
        raise

def parse_xml_file(cursor: sqlite3.Cursor, xml_file: str) -> None:
    """Parse the XML file and insert data into the pointz_aggregator_data table."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for user in root.findall('user'):
            uid = user.get('uid', '')
            name = user.find('name')
            first_name = name.get('first', '') if name is not None else ''
            last_name = name.get('last', '') if name is not None else ''

            cards = user.find('cards')
            if cards is not None:
                for card in cards.findall('card'):
                    card_number = card.get('number', '')
                    bonus_programm = card.find('bonusprogramm').text if card.find('bonusprogramm') is not None else ''

                    activities = card.find('activities')
                    if activities is not None:
                        for activity in activities.findall('activity'):
                            if activity.get('type') == 'Flight':
                                flight_code = activity.find('Code').text if activity.find('Code') is not None else ''
                                flight_date = activity.find('Date').text if activity.find('Date') is not None else ''
                                departure = activity.find('Departure').text if activity.find('Departure') is not None else ''
                                arrival = activity.find('Arrival').text if activity.find('Arrival') is not None else ''
                                fare = activity.find('Fare').text if activity.find('Fare') is not None else ''

                                cursor.execute('''
                                    INSERT INTO pointz_aggregator_data (
                                        UserUID, FirstName, LastName, CardNumber, BonusProgramm,
                                        FlightCode, FlightDate, Departure, Arrival, Fare
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (uid, first_name, last_name, card_number, bonus_programm,
                                      flight_code, flight_date, departure, arrival, fare))
    except FileNotFoundError:
        logger.error(f"XML file not found: {xml_file}")
        raise
    except ET.ParseError:
        logger.error(f"Invalid XML format in file: {xml_file}")
        raise
    except Exception as e:
        logger.error(f"Error parsing XML file: {e}")
        raise

def parse_yaml_file(cursor: sqlite3.Cursor, yaml_file: str) -> None:
    """Parse the YAML file and insert data into the skyteam_data table."""
    try:
        yaml = YAML(typ='safe')  # Use ruamel.yaml with safe loading
        with open(yaml_file, 'r', encoding='utf-8') as file:
            start_time = time.time()
            logger.info(f"Starting YAML parsing for {yaml_file}")
            data = yaml.load(file)
            if not data:
                logger.warning(f"YAML file {yaml_file} is empty")
                return

            rows = []
            for flight_date, flights in data.items():
                for flight_number, flight_info in flights.items():
                    departure = flight_info.get('FROM', '')
                    arrival = flight_info.get('TO', '')
                    status = flight_info.get('STATUS', '')
                    ff_data = flight_info.get('FF', {})

                    for ff_number, ff_info in ff_data.items():
                        ff_program = ff_number.split()[0] if ff_number and ' ' in ff_number else ''
                        travel_class = ff_info.get('CLASS', '')
                        fare = ff_info.get('FARE', '')

                        rows.append((
                            flight_date, flight_number, ff_program, ff_number,
                            travel_class, fare, departure, arrival, status
                        ))

            if rows:
                cursor.executemany('''
                    INSERT INTO skyteam_data (
                        FlightDate, FlightNumber, FFProgram, FFNumber, 
                        TravelClass, Fare, Departure, Arrival, Status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', rows)

            elapsed = time.time() - start_time
            logger.info(f"Completed YAML parsing in {elapsed:.2f} seconds")
    except FileNotFoundError:
        logger.error(f"YAML file not found: {yaml_file}")
        raise
    except YAMLError as e:
        logger.error(f"Invalid YAML format in file {yaml_file}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise

def parse_skyteam_timetable(cursor: sqlite3.Cursor, pdf_file: str) -> None:
    """Parse the SkyTeam timetable from the extracted PDF text and insert into the table."""
    try:
        reader = PdfReader(pdf_file)
        text = ""
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {e}")
        raise

    lines = text.splitlines()
    i = 0
    from_city = from_country = from_airport = None
    to_city = to_country = to_airport = None
    seen = set()
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('FROM:'):
            match = re.search(r'FROM:\s*(.+?),\s*(.+?)\s*(\w{3})', line)
            if match:
                from_city, from_country, from_airport = match.groups()
            seen = set()  # Reset seen for new route
        elif line.startswith('TO:'):
            match = re.search(r'TO:\s*(.+?),\s*(.+?)\s*(\w{3})', line)
            if match:
                to_city, to_country, to_airport = match.groups()
        elif re.match(r'\d{2} \w{3}  -  \d{2} \w{3}', line):
            if from_city and to_city:
                validity = line.strip()
                i += 1
                line = lines[i].strip()
                parts = re.split(r'\s+', line)
                if len(parts) >= 2:
                    days = parts[0]
                    dep_time = parts[1]
                else:
                    days = line
                    i += 1
                    dep_time = lines[i].strip()
                i += 1
                arr_time = lines[i].strip()
                i += 1
                flight = lines[i].strip()
                i += 1
                aircraft = lines[i].strip()
                i += 1
                travel_time = lines[i].strip()
                tuple_key = (validity, days, dep_time, arr_time, flight, aircraft, travel_time)
                if tuple_key not in seen:
                    seen.add(tuple_key)
                    cursor.execute('''
                        INSERT INTO skyteam_timetable (
                            FromCity, FromCountry, FromAirport, ToCity, ToCountry, ToAirport,
                            Validity, Days, DepTime, ArrTime, Flight, Aircraft, TravelTime
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (from_city, from_country, from_airport, to_city, to_country, to_airport,
                          validity, days, dep_time, arr_time, flight, aircraft, travel_time))
        i += 1

def main():
    """Main function to orchestrate database creation and file parsing."""
    try:
        # Ensure the Data directory exists and files are present
        for file in [CSV_FILE, TAB_FILE, XML_FILE, YAML_FILE, PDF_FILE]:
            if not Path(file).is_file():
                logger.error(f"File not found: {file}")
                return

        # Create database connection
        conn, cursor = create_database_connection(DB_FILE)

        # Create tables
        create_boarding_data_table(cursor)
        create_sirena_data_table(cursor)
        create_pointz_aggregator_table(cursor)
        create_skyteam_data_table(cursor)
        create_skyteam_timetable_table(cursor)

        # Process files based on flags
        if PROCESS_CSV:
            if CLEAR_CSV:
                logger.info("Clearing boarding_data table")
                cursor.execute('DELETE FROM boarding_data')
            logger.info(f"Processing CSV file: {CSV_FILE}")
            parse_csv_file(cursor, CSV_FILE)
        if PROCESS_TAB:
            if CLEAR_TAB:
                logger.info("Clearing sirena_data table")
                cursor.execute('DELETE FROM sirena_data')
            logger.info(f"Processing TAB file: {TAB_FILE}")
            parse_tab_file(cursor, TAB_FILE)
        if PROCESS_XML:
            if CLEAR_XML:
                logger.info("Clearing pointz_aggregator_data table")
                cursor.execute('DELETE FROM pointz_aggregator_data')
            logger.info(f"Processing XML file: {XML_FILE}")
            parse_xml_file(cursor, XML_FILE)
        if PROCESS_YAML:
            if CLEAR_YAML:
                logger.info("Clearing skyteam_data table")
                cursor.execute('DELETE FROM skyteam_data')
            logger.info(f"Processing YAML file: {YAML_FILE}")
            parse_yaml_file(cursor, YAML_FILE)
        if PROCESS_PDF:
            if CLEAR_PDF:
                logger.info("Clearing skyteam_timetable table")
                cursor.execute('DELETE FROM skyteam_timetable')
            logger.info(f"Processing PDF file: {PDF_FILE}")
            parse_skyteam_timetable(cursor, PDF_FILE)

        # Commit changes and close connection
        conn.commit()
        logger.info("Data successfully inserted into the database.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
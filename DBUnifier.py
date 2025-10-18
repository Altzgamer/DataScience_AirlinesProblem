import sqlite3
import re
from typing import Tuple, Set, Dict, Optional
from collections import defaultdict

def find(parent, x):
    root = x
    while parent[root] != root:
        root = parent[root]
    # Path compression
    while x != root:
        next_node = parent[x]
        parent[x] = root
        x = next_node
    return root

def union(parent, x, y):
    px = find(parent, x)
    py = find(parent, y)
    if px != py:
        parent[px] = py

def merge_duplicates(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all data
    cursor.execute("SELECT * FROM Person")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not rows:
        print("No data in Person table.")
        conn.close()
        return

    # Create dict of rows by PersonID
    data = {row[0]: dict(zip(columns, row)) for row in rows}

    # Scalars and sets
    scalars = ['FirstName', 'MiddleName', 'LastName', 'Sex', 'BirthDate']
    set_cols = [col for col in columns if col not in ['PersonID'] + scalars]

    # Initialize union-find
    parent = {pid: pid for pid in data}

    # Match on shared travel documents
    doc_to_ids = defaultdict(list)
    for pid, row in data.items():
        docs = row['TravelDocuments'].split(',') if row['TravelDocuments'] else []
        for doc in [d.strip() for d in docs if d.strip()]:
            doc_to_ids[doc].append(pid)

    for ids in doc_to_ids.values():
        for i in range(1, len(ids)):
            union(parent, ids[0], ids[i])

    # Match on shared loyalty numbers
    loyalty_to_ids = defaultdict(list)
    for pid, row in data.items():
        loyalties = row['LoyaltyNumbers'].split(',') if row['LoyaltyNumbers'] else []
        for loy in [l.strip() for l in loyalties if l.strip()]:
            loyalty_to_ids[loy].append(pid)

    for ids in loyalty_to_ids.values():
        for i in range(1, len(ids)):
            union(parent, ids[0], ids[i])

    # Match on same LastName + BirthDate, with compatible FirstName
    name_birth_to_ids = defaultdict(list)
    for pid, row in data.items():
        if row['LastName'] and row['BirthDate']:
            key = (row['LastName'].strip().upper(), row['BirthDate'].strip())
            name_birth_to_ids[key].append(pid)

    for ids in name_birth_to_ids.values():
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    pid1, pid2 = ids[i], ids[j]
                    first1 = data[pid1]['FirstName'].strip().upper()
                    first2 = data[pid2]['FirstName'].strip().upper()
                    if first1 == first2 or not first1 or not first2:
                        union(parent, pid1, pid2)

    # Find components
    groups = defaultdict(list)
    for pid in parent:
        root = find(parent, pid)
        groups[root].append(pid)

    # Merge each group
    cursor.execute('BEGIN')
    for group in groups.values():
        if len(group) <= 1:
            continue

        # Sort group for consistency
        group.sort()

        # Collect sub data
        sub_rows = {pid: data[pid] for pid in group}

        merged = {}

        # Merge scalars: prefer the longest non-empty
        for col in scalars:
            candidates = [sub_rows[pid][col].strip() for pid in group if sub_rows[pid][col].strip()]
            if candidates:
                merged[col] = max(candidates, key=len)
            else:
                merged[col] = ''

        # Merge sets: union unique items
        for col in set_cols:
            all_items = set()
            for pid in group:
                items = sub_rows[pid][col].split(',') if sub_rows[pid][col] else []
                all_items.update(i.strip() for i in items if i.strip())
            merged[col] = ','.join(sorted(all_items))

        # Keep the smallest PersonID
        min_id = min(group)

        # Update the min_id row
        set_clause = ', '.join(f"{col} = ?" for col in scalars + set_cols)
        values = [merged[col] for col in scalars + set_cols]
        cursor.execute(f"UPDATE Person SET {set_clause} WHERE PersonID = ?", values + [min_id])

        # Delete the other rows
        del_ids = [pid for pid in group if pid != min_id]
        if del_ids:
            placeholders = ','.join('?' for _ in del_ids)
            cursor.execute(f"DELETE FROM Person WHERE PersonID IN ({placeholders})", del_ids)

    conn.commit()
    conn.close()
    print("Duplicates merged successfully.")

def transliterate(name: str) -> str:
    """Transliterate Cyrillic to Latin."""
    trans_table = {
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'YO', 'Ж': 'ZH', 'З': 'Z', 'И': 'I',
        'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T',
        'У': 'U', 'Ф': 'F', 'Х': 'KH', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SHCH', 'Ъ': '', 'Ы': 'Y', 'Ь': '',
        'Э': 'E', 'Ю': 'YU', 'Я': 'YA',
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i',
        'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
        'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya',
    }
    return ''.join(trans_table.get(c, c) for c in name)

def normalize_name(name: str) -> str:
    """Normalize a name by transliterating, converting to uppercase and removing extra spaces."""
    if not name or name.lower() == 'not presented':
        return ''
    name = transliterate(name)
    return re.sub(r'\s+', ' ', name.strip().upper())

def normalize_document(doc: str) -> str:
    """Normalize document number by removing spaces and converting to uppercase."""
    if not doc or doc.lower() == 'not presented':
        return ''
    return doc.replace(' ', '').upper()

def create_person_table(cursor: sqlite3.Cursor) -> None:
    """Create the Person table to store consolidated passenger data."""
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Person (
            PersonID INTEGER PRIMARY KEY AUTOINCREMENT,
            FirstName TEXT,
            MiddleName TEXT,
            LastName TEXT,
            Sex TEXT,
            BirthDate TEXT,
            TravelDocuments TEXT,
            LoyaltyNumbers TEXT,
            TicketNumbers TEXT,
            BookingCodes TEXT,
            FlightHistory TEXT,
            DepartureCities TEXT,
            ArrivalCities TEXT,
            LoyaltyPrograms TEXT,
            Meals TEXT,
            TravelClasses TEXT,
            FareBases TEXT,
            Baggages TEXT,
            Seats TEXT,
            Statuses TEXT,
            DepartureCountries TEXT,
            ArrivalCountries TEXT,
            AdditionalInfos TEXT,
            AgentInfos TEXT
        )
    ''')

def get_person_key(first_name: str, last_name: str, birth_date: str, travel_doc: str) -> Tuple[str, ...]:
    """Generate a key for matching persons across tables."""
    return (
        normalize_name(first_name),
        normalize_name(last_name),
        birth_date.strip() if birth_date else '',
        normalize_document(travel_doc)
    )

def merge_person_data(db_path: str) -> None:
    """Merge data from multiple tables into a single Person table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the Person table
    create_person_table(cursor)

    # Dictionary to store person data by matching key
    persons: Dict[Tuple[str, ...], Dict] = {}

    # Process boarding_data
    for row in cursor.execute("SELECT * FROM boarding_data"):
        first_name, middle_name, last_name, sex, birth_date, doc, booking, ticket, baggage, flight_date, flight_time, flight_num, codeshare, dest = row
        key = get_person_key(first_name, last_name, birth_date, doc)
        if key not in persons:
            persons[key] = {
                'FirstName': normalize_name(first_name),
                'MiddleName': normalize_name(middle_name),
                'LastName': normalize_name(last_name),
                'Sex': sex,
                'BirthDate': birth_date,
                'TravelDocuments': set(),
                'LoyaltyNumbers': set(),
                'TicketNumbers': set(),
                'BookingCodes': set(),
                'FlightHistory': set(),
                'DepartureCities': set(),
                'ArrivalCities': set(),
                'LoyaltyPrograms': set(),
                'Meals': set(),
                'TravelClasses': set(),
                'FareBases': set(),
                'Baggages': set(),
                'Seats': set(),
                'Statuses': set(),
                'DepartureCountries': set(),
                'ArrivalCountries': set(),
                'AdditionalInfos': set(),
                'AgentInfos': set()
            }
        person = persons[key]
        if doc and doc.lower() != 'not presented':
            person['TravelDocuments'].add(normalize_document(doc))
        if ticket and ticket.lower() != 'not presented':
            person['TicketNumbers'].add(normalize_document(ticket))
        if booking and booking.lower() != 'not presented':
            person['BookingCodes'].add(normalize_document(booking))
        flight_str = f"{flight_num} {flight_date} {flight_time}"
        if codeshare and codeshare.lower() != 'not presented':
            flight_str += f" ({codeshare})"
        person['FlightHistory'].add(flight_str)
        person['ArrivalCities'].add(dest)
        if baggage and baggage.lower() != 'not presented':
            person['Baggages'].add(baggage)

    # Build name_to_first_key for fast lookup in boarding_pass_xls
    name_to_first_key: Dict[Tuple[str, str], Tuple[str, ...]] = {}
    for key in persons:
        name = (key[0], key[1])
        if name not in name_to_first_key:
            name_to_first_key[name] = key

    # Process boarding_pass_xls
    for row in cursor.execute("SELECT * FROM boarding_pass_xls"):
        title, name, loyalty_prog, loyalty_num, fare_class, flight_num, dep_city, arr_city, dep_airport, arr_airport, flight_date, flight_time, pnr, eticket = row
        # Parse name (e.g., "LAVROV EVGENIY G" -> First: EVGENIY, Middle: G, Last: LAVROV)
        name_parts = normalize_name(name).split()
        first_name = name_parts[1] if len(name_parts) > 1 else ''
        middle_name = name_parts[2] if len(name_parts) > 2 else ''
        last_name = name_parts[0] if name_parts else ''
        name = (first_name, last_name)
        if name in name_to_first_key:
            best_key = name_to_first_key[name]
        else:
            best_key = (first_name, last_name, '', '')
            persons[best_key] = {
                'FirstName': first_name,
                'MiddleName': middle_name,
                'LastName': last_name,
                'Sex': '',
                'BirthDate': '',
                'TravelDocuments': set(),
                'LoyaltyNumbers': set(),
                'TicketNumbers': set(),
                'BookingCodes': set(),
                'FlightHistory': set(),
                'DepartureCities': set(),
                'ArrivalCities': set(),
                'LoyaltyPrograms': set(),
                'Meals': set(),
                'TravelClasses': set(),
                'FareBases': set(),
                'Baggages': set(),
                'Seats': set(),
                'Statuses': set(),
                'DepartureCountries': set(),
                'ArrivalCountries': set(),
                'AdditionalInfos': set(),
                'AgentInfos': set()
            }
            name_to_first_key[name] = best_key
        person = persons[best_key]
        if loyalty_num:
            person['LoyaltyNumbers'].add(normalize_document(loyalty_num))
        if eticket:
            person['TicketNumbers'].add(normalize_document(eticket))
        if pnr:
            person['BookingCodes'].add(normalize_document(pnr))
        person['FlightHistory'].add(f"{flight_num} {flight_date} {flight_time}")
        person['DepartureCities'].add(dep_city)
        person['ArrivalCities'].add(arr_city)
        if loyalty_prog:
            person['LoyaltyPrograms'].add(loyalty_prog)
        if fare_class:
            person['TravelClasses'].add(fare_class)

    # Process sirena_data
    for row in cursor.execute("SELECT * FROM sirena_data"):
        pax_name, birth_date, dep_date, dep_time, arr_date, arr_time, flight_code, from_airport, dest, code, eticket, travel_doc, seat, meal, trv_cls, fare, baggage, pax_info, agent_info = row
        name_parts = normalize_name(pax_name).split()
        first_name = name_parts[1] if len(name_parts) > 1 else ''
        middle_name = name_parts[2] if len(name_parts) > 2 else ''
        last_name = name_parts[0] if name_parts else ''
        key = get_person_key(first_name, last_name, birth_date, travel_doc)
        if key not in persons:
            persons[key] = {
                'FirstName': first_name,
                'MiddleName': middle_name,
                'LastName': last_name,
                'Sex': '',
                'BirthDate': birth_date,
                'TravelDocuments': set(),
                'LoyaltyNumbers': set(),
                'TicketNumbers': set(),
                'BookingCodes': set(),
                'FlightHistory': set(),
                'DepartureCities': set(),
                'ArrivalCities': set(),
                'LoyaltyPrograms': set(),
                'Meals': set(),
                'TravelClasses': set(),
                'FareBases': set(),
                'Baggages': set(),
                'Seats': set(),
                'Statuses': set(),
                'DepartureCountries': set(),
                'ArrivalCountries': set(),
                'AdditionalInfos': set(),
                'AgentInfos': set()
            }
        person = persons[key]
        if travel_doc and travel_doc.lower() != 'not presented':
            person['TravelDocuments'].add(normalize_document(travel_doc))
        if eticket:
            person['TicketNumbers'].add(normalize_document(eticket))
        if code:
            person['BookingCodes'].add(normalize_document(code))
        person['FlightHistory'].add(f"{flight_code} {dep_date} {dep_time}")
        person['DepartureCities'].add(from_airport)
        person['ArrivalCities'].add(dest)
        if seat:
            person['Seats'].add(seat)
        if meal:
            person['Meals'].add(meal)
        if trv_cls:
            person['TravelClasses'].add(trv_cls)
        if fare:
            person['FareBases'].add(fare)
        if baggage:
            person['Baggages'].add(baggage)
        if pax_info and pax_info.lower() != 'not presented':
            person['AdditionalInfos'].add(pax_info)
        if agent_info and agent_info.lower() != 'not presented':
            person['AgentInfos'].add(agent_info)

    # Process pointz_aggregator_data
    for row in cursor.execute("SELECT * FROM pointz_aggregator_data"):
        user_uid, first_name, last_name, card_num, bonus_prog, flight_code, flight_date, dep, arr, fare = row
        key = get_person_key(first_name, last_name, '', card_num)
        if key not in persons:
            persons[key] = {
                'FirstName': normalize_name(first_name),
                'MiddleName': '',
                'LastName': normalize_name(last_name),
                'Sex': '',
                'BirthDate': '',
                'TravelDocuments': set(),
                'LoyaltyNumbers': set(),
                'TicketNumbers': set(),
                'BookingCodes': set(),
                'FlightHistory': set(),
                'DepartureCities': set(),
                'ArrivalCities': set(),
                'LoyaltyPrograms': set(),
                'Meals': set(),
                'TravelClasses': set(),
                'FareBases': set(),
                'Baggages': set(),
                'Seats': set(),
                'Statuses': set(),
                'DepartureCountries': set(),
                'ArrivalCountries': set(),
                'AdditionalInfos': set(),
                'AgentInfos': set()
            }
        person = persons[key]
        if card_num:
            person['LoyaltyNumbers'].add(normalize_document(card_num))
        person['FlightHistory'].add(f"{flight_code} {flight_date}")
        person['DepartureCities'].add(dep)
        person['ArrivalCities'].add(arr)
        if bonus_prog:
            person['LoyaltyPrograms'].add(bonus_prog)
        if fare:
            person['FareBases'].add(fare)

    # Build ff_to_first_key for fast lookup in skyteam_data
    ff_to_first_key: Dict[str, Tuple[str, ...]] = {}
    for key in persons:
        for ff in persons[key]['LoyaltyNumbers']:
            if ff not in ff_to_first_key:
                ff_to_first_key[ff] = key



    # Process frequent_flyer_profiles
    for row in cursor.execute("SELECT * FROM frequent_flyer_profiles"):
        nick, sex, first_name, last_name, travel_docs, loyalties = row
        key = get_person_key(first_name, last_name, '', travel_docs)
        if key not in persons:
            persons[key] = {
                'FirstName': normalize_name(first_name),
                'MiddleName': '',
                'LastName': normalize_name(last_name),
                'Sex': sex,
                'BirthDate': '',
                'TravelDocuments': set(),
                'LoyaltyNumbers': set(),
                'TicketNumbers': set(),
                'BookingCodes': set(),
                'FlightHistory': set(),
                'DepartureCities': set(),
                'ArrivalCities': set(),
                'LoyaltyPrograms': set(),
                'Meals': set(),
                'TravelClasses': set(),
                'FareBases': set(),
                'Baggages': set(),
                'Seats': set(),
                'Statuses': set(),
                'DepartureCountries': set(),
                'ArrivalCountries': set(),
                'AdditionalInfos': set(),
                'AgentInfos': set()
            }
        person = persons[key]
        if travel_docs:
            person['TravelDocuments'].add(normalize_document(travel_docs))
        if loyalties:
            person['LoyaltyNumbers'].add(normalize_document(loyalties))

    # Pre-fetch frequent_flyer_profiles into a dict for fast lookup
    profiles: Dict[str, Tuple[str, str, str]] = {}
    for row in cursor.execute("SELECT Nick, FirstName, LastName, TravelDocuments FROM frequent_flyer_profiles"):
        nick, first_name, last_name, travel_docs = row
        profiles[nick] = (normalize_name(first_name), normalize_name(last_name), normalize_document(travel_docs))

    # Process frequent_flyer_flights
    for row in cursor.execute("SELECT * FROM frequent_flyer_flights"):
        nick, flight_date, flight, codeshare, dep_city, dep_airport, dep_country, arr_city, arr_airport, arr_country = row
        # Match by nick in profiles
        if nick not in profiles:
            continue
        first_name, last_name, travel_docs = profiles[nick]
        key = get_person_key(first_name, last_name, '', travel_docs)
        if key not in persons:
            continue
        person = persons[key]
        flight_str = f"{flight} {flight_date}"
        if codeshare:
            flight_str += f" ({codeshare})"
        person['FlightHistory'].add(flight_str)
        person['DepartureCities'].add(dep_city)
        person['ArrivalCities'].add(arr_city)
        if dep_country:
            person['DepartureCountries'].add(dep_country)
        if arr_country:
            person['ArrivalCountries'].add(arr_country)

    # Prepare data for batch insert
    insert_data = []
    for person in persons.values():
        insert_data.append((
            person['FirstName'],
            person['MiddleName'],
            person['LastName'],
            person['Sex'],
            person['BirthDate'],
            ','.join(person['TravelDocuments']),
            ','.join(person['LoyaltyNumbers']),
            ','.join(person['TicketNumbers']),
            ','.join(person['BookingCodes']),
            ','.join(person['FlightHistory']),
            ','.join(person['DepartureCities']),
            ','.join(person['ArrivalCities']),
            ','.join(person['LoyaltyPrograms']),
            ','.join(person['Meals']),
            ','.join(person['TravelClasses']),
            ','.join(person['FareBases']),
            ','.join(person['Baggages']),
            ','.join(person['Seats']),
            ','.join(person['Statuses']),
            ','.join(person['DepartureCountries']),
            ','.join(person['ArrivalCountries']),
            ','.join(person['AdditionalInfos']),
            ','.join(person['AgentInfos'])
        ))

    # Batch insert
    if insert_data:
        cursor.executemany('''
            INSERT INTO Person (
                FirstName, MiddleName, LastName, Sex, BirthDate,
                TravelDocuments, LoyaltyNumbers, TicketNumbers, BookingCodes,
                FlightHistory, DepartureCities, ArrivalCities,
                LoyaltyPrograms, Meals, TravelClasses, FareBases,
                Baggages, Seats, Statuses, DepartureCountries, ArrivalCountries,
                AdditionalInfos, AgentInfos
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', insert_data)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    db_path = 'DataBase.db'  # Use 'Persons.db' as per the merge_duplicates call in the query
    merge_person_data(db_path)
    merge_duplicates(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()
    print("Database unified successfully.")
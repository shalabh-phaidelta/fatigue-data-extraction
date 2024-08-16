import re
import sqlite3

conn = sqlite3.connect('ftg.db')

def extract_table_from_text(file_path, count=0):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    start_extracting = False
    table_lines = []
    
    for line in lines:
        # Check for the start of the table using the keyword "DAMAGE ORDER"
        if "DAMAGE ORDER" in line:
            count += 1
            if count > 2:
                print("extracting... ",line)
                start_extracting = True
            continue
        
        # print(line)
        # Check for the end of the table using the end marker "FF CR LF"
        # if start_extracting and line.strip().startswith("SACS (2023)"):
        #     print('breaking!...', line)
        #     break
        
        # Collect table lines
        if start_extracting and line!='\n':
            table_lines.append(line.strip())
    
    # Process the table lines to extract data
    table_data = []
    for line in table_lines:
        # Use regular expressions to split the line into columns
        columns = re.split(r'\s{2,}', line)
        table_data.append(columns)
    
    return table_data

def convert_to_number(string, search_char):
    index = string.find(search_char)
    first_part: string = string[:index]
    if search_char == '-':
        second_part = '-' + string[index+1:]
    else:
        second_part = string[index+1:]
    return first_part + 'e' + second_part

def clean_extracted_tbl(tbl_data:list[str]):
    cleaned_tbl = []
    allowed_pattern = r'\d{4}-\d{4}'
    for item in tbl_data:
        # if 'TUB' in item:
        #     continue
        if 'INFINITE' in item:
            continue
        if re.match(allowed_pattern, item[0]):
            # print("allowing line", item)
            if '-' in item[9]:
                item[9] = convert_to_number(item[9], '-')
            elif '+' in item[9]:
                item[9] = convert_to_number(item[9], '+')

            if '-' in item[11]:
                item[11] = convert_to_number(item[11], '-')
            elif '+' in item[11]:
                item[11] = convert_to_number(item[11], '+')
            
            cleaned_tbl.append(item)
    return cleaned_tbl

def create_tbl(conn):
    cursor = conn.cursor()
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS ftg2 (
        MEMBER TEXT NOT NULL,
        JOINT TEXT NOT NULL,
        GROUP_ID TEXT,
        TYPE_ID TEXT,
        F_AXIAL TEXT,
        F_MAJOR TEXT,
        F_MINOR TEXT,
        W_AXIAL TEXT,
        W_MAJOR TEXT,
        DAMAGE TEXT,
        LOC TEXT,
        SVC_LIFE TEXT,
        PRIMARY KEY (MEMBER, JOINT)
        )
        '''
    )

def insert_into_db(tbl_data:list[str], conn: sqlite3.Connection):
    cursor = conn.cursor()
    for row in tbl_data:
        cursor.execute('''
                       INSERT INTO ftg2 (MEMBER, JOINT, GROUP_ID, TYPE_ID, F_AXIAL, F_MAJOR, F_MINOR, W_AXIAL, W_MAJOR, DAMAGE, LOC, SVC_LIFE)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', row)
    conn.commit()
    print("Data inserted into ftg tbl")
    conn.close()    

####
def test():
    # test = [
    #         ['E057 2272-E057 E03', 'TUB', '21.91', '0.800', '0.00', '0.00', '0.00', '0.00', '.0000000', 'TR', 'INFINITE'],
    #         ['*', '*', '*', 'M E M B E R', 'F A T I G U E', 'R E P O R T', '*', '*', '*'],
    #         ['ORIGINAL', 'CHORD', 'REQUIRED'],
    #         ['0084-0335', '0335', 'P23', 'WF', '0.00', '0.00', '0.00', '0.00', '0.00', '.0000000', 'B', 'INFINITE'],
    #         ['0335', '0335', 'P23', 'WF', '0.00', '0.00', '0.00', '0.00', '0.00', '.0000000', 'B', 'INFINITE']
    #     ]

    # cleaned = clean_extracted_tbl(test)
    # for row in cleaned:
    #     print(row)

    num = ".1234-3"
    str = convert_to_number(num,'-')
    print(float(str))

def main():
    # Usage
    file_path = 'ftglst.full_f'
    table_data = extract_table_from_text(file_path)
    cleaned_data = clean_extracted_tbl(table_data)
    
    print('creating tbl')
    create_tbl(conn)
    print('tbl created...inserting data')
    insert_into_db(cleaned_data, conn)
    # Display the extracted table data
    # for row in cleaned_data:
    #     print(row)

if __name__ == "__main__":
    main()
    # test()

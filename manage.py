from flask import Flask, send_file, render_template
import pandas as pd
import os
import requests
from fuzzywuzzy import process
from flask import Flask, render_template
import pandas as pd
import os
import psycopg2
from psycopg2 import sql
from datetime import datetime

app = Flask(__name__)

try:
    # %%
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Data Generation initiated... Started at {current_time}. [Expected Time : 7 minutes ]")
    # %%
    import pandas as pd
    from sqlalchemy import create_engine

    # Define your connection parameters
    engine = create_engine('postgresql+psycopg2://postgres:admin%40123@localhost:5432/postgres')

    # SQL to fetch data
    query = "SELECT * FROM product_groups"
    key_df = pd.read_sql(query, engine)

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Opportunity'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","deal_pipeline","items.item_code","items.item_group","items.item_name","customer_name"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
            'filters': '[["status", "not in", ["Order Lost","Order Won","Converted","Closed","Lost"]]]'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        opp_df = pd.json_normalize(all_data)
        print('fields are correct')
    else:
        columns = ["ID", "Deal Pipeline", "Item Code (Opportunity Item)", "Item Group (Opportunity Item)", "Item Name (Opportunity Item)", "Customer Name"]
        opp_df = pd.DataFrame(columns=columns)


    # %%
    opp_df.head()

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Customer'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","customer_group","territory"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        customer = pd.json_normalize(all_data)
        print('fields are correct')
    else:
        columns = ["name","deal_pipeline","items.item_code","items.item_group","items.item_name","customer_name"]
        customer = pd.DataFrame(columns=columns)

    # %%
    customer

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Serial No'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","customer_instrument_id","item_code","item_name","customer","customer_name","territory","serial_no","amc_expiry_date","item_group"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
            'filters': '[["item_group", "in", ["Tablet Dissolution Tester - Machine", "Tablet Hardness Tester - Machine"]]]'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        serial_df = pd.json_normalize(all_data)
        print('fields are correct')
    else:
        columns = ["name","customer_instrument_id","item_code","item_name","customer","customer_name","territory","serial_no","amc_expiry_date","item_group"]
        serial_df = pd.DataFrame(columns=columns)

    # %%
    serial_df

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Sales Order'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","sales_team.sales_person","customer","customer_name","items.item_code","items.item_group","items.item_name","items.qty"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        sales_df = pd.json_normalize(all_data)
        print('fields are correct')
    else:
        columns = ["ID", "Sales Person (Sales Team)", "Customer", "Customer Name", "Item Code (Sales Order Item)", "Item Group (Sales Order Item)", "Item Name (Sales Order Item)", "Quantity (Sales Order Item)"]
        sales_df = pd.DataFrame(columns=columns)


    # %%
    sales_df

    # %%
    ### Step 1: identify the key item group from serial nomber by comparing it to serial_df

    # %%
    customer.rename(columns = {'name':'customer'},inplace = True)
    # customer.rename(columns = {'customer_group':'Customer Group'},inplace = True)

    customer.head()

    # %%
    serial_df.head()

    # %%
    serial_df = pd.merge(serial_df, customer,on = 'customer', how = 'left')
    serial_df

    # %%
    serial_df.rename(columns = {'territory_y':'territory'}, inplace = True)
    # # serial_df.rename(columns = {'Customer Group_x':'Customer Group'}, inplace = True)

    # serial_df.head()

    # %%
    serial_df['amc_expiry_date'] = pd.to_datetime(serial_df['amc_expiry_date'], errors='coerce')
    serial_df

    # %%
    from fuzzywuzzy import process

    # %%
    # Function to find the best match for each key_df item_group in serial_df item_group
    def match_item_group(row, serial_item_groups):
        match, score = process.extractOne(row['item_group'], serial_item_groups)
        return match if score >= 80 else None  # Adjust threshold as needed

    # Apply the fuzzy matching for each row in key_df
    serial_item_groups = serial_df['item_group'].tolist()
    key_df['matched_item_group'] = key_df.apply(match_item_group, serial_item_groups=serial_item_groups, axis=1)


    # %%
    key_df

    # %%
    merged_df = pd.merge(key_df, serial_df, left_on='matched_item_group', right_on='item_group', how='inner')


    # %%


    # %%
    merged_df.head()

    # %%
    merged_df.shape

    # %%
    grouped_df = merged_df.groupby(['item_group_x', 'item_group_y', 'customer', 'item_code_x','customer_group','territory']).size().reset_index(name='count_tam')

    # %%
    grouped_df.head()

    # %%
    grouped_df.shape

    # %%
    opp_df.head()

    # %%
    # Group by 'Item Code' and 'Customer Name' and count the occurrences
    grouped_opp_df = opp_df.groupby(['item_code', 'customer_name']).size().reset_index(name='count_opp')
    grouped_opp_df.head()

    # %%
    grouped_opp_df.shape

    # %%
    sales_df.head()

    # %%
    grouped_sales_df = sales_df.groupby(['item_code', 'customer_name'])['qty'].sum().reset_index(name='count_sales')

    grouped_sales_df.head()

    # %%
    grouped_df.head()

    # %%
    # First merge grouped_df with grouped_sales_df on item_code and Customer Name
    merged_df_sales = pd.merge(grouped_df, grouped_sales_df, 
                            left_on=['item_code_x', 'customer'], 
                            right_on=['item_code', 'customer_name'], 
                            how='left')

    # Now merge the result with grouped_opp_df on item_code and Customer Name
    final_merged_df = pd.merge(merged_df_sales, grouped_opp_df, 
                            left_on=['item_code', 'customer_name'], 
                            right_on=['item_code', 'customer_name'], 
                            how='left')

    # Display the result
    final_merged_df.head()

    # %%
    final_merged_df.head()

    # %%
    # Grouping by the desired columns and summing the count columns
    grouped_final_df = final_merged_df.groupby(['item_group_x', 'item_group_y', 'customer','customer_group','territory' ,'item_code_x']).agg(
        {
            'count_tam': 'sum',   # Summing count_tam
            'count_sales': 'sum',       # Summing count from grouped_sales_df
            'count_opp': 'sum'    # Summing count_opp from grouped_opp_df
        }
    ).reset_index()

    # Display the grouped dataframe
    grouped_final_df.head()

    # %%
    # Rename the count columns in the grouped_final_df
    grouped_final_df.rename(columns={
        'count_tam': 'Tam Count (Total Quantity Available with Customer)',
        'count_sales': 'Count Sales (According to the Quantity Booked)',
        'count_opp': 'Count Of Open Opportunity'
    }, inplace=True)

    final_merged_df = final_merged_df.drop(columns=['item_code', 'customer_name', 'item_group_y'])
    final_merged_df[['count_tam', 'count_sales', 'count_opp']] = final_merged_df[['count_tam', 'count_sales', 'count_opp']].fillna(0)
    final_merged_df = final_merged_df.sort_values(by='count_tam', ascending=False)

    import psycopg2
    import pandas as pd

    # Define connection parameters
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='admin@123'
    )

    # Create a cursor object
    cur = conn.cursor()

    # Table creation SQL
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS final_merged_data (
        item_group_x VARCHAR(255),
        customer VARCHAR(255),
        item_code_x VARCHAR(255),
        customer_group VARCHAR(255),
        territory VARCHAR(255),
        count_tam INTEGER,
        count_sales INTEGER,
        count_opp INTEGER
    );
    '''

    # Execute table creation
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully.")

    truncate_query = "TRUNCATE TABLE final_merged_data;"
    cur.execute(truncate_query)

    insert_query = '''
    INSERT INTO final_merged_data (item_group_x, customer, item_code_x, customer_group, territory, count_tam, count_sales, count_opp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    ''' 

    for _, row in final_merged_df.iterrows():
        cur.execute(insert_query, (
            row['item_group_x'],
            row['customer'],
            row['item_code_x'],
            row['customer_group'],
            row['territory'],
            int(row['count_tam']),
            int(row['count_sales']),
            int(row['count_opp'])
        ))

    conn.commit()
    print("Excel Data inserted successfully - SPARES .")

    # Close cursor and connection
    cur.close()
    conn.close()


#before this
    grouped_by_item_code = grouped_final_df.groupby('item_code_x', as_index=False).agg({'Count Of Open Opportunity': 'sum' })

    grouped_by_item_code['Date'] = datetime.now().date()
    grouped_by_item_code['Date Time'] = datetime.now()

    from datetime import datetime, timedelta

    # yesterday = datetime.now() - t*imedelta(days=6)
    # grouped_by_item_code['Date'] = yesterday.date()  
    # grouped_by_item_code['Date Time'] = yesterday 
    df =grouped_by_item_code.copy()
    conn = None
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='admin@123'
        )
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS opportunity_summary (
            item_code_x VARCHAR(255),
            count_of_open_opportunity INTEGER,
            date DATE,
            date_time TIMESTAMP
        )
        """
        cur.execute(create_table_query)
        conn.commit()

        for index, row in df.iterrows():
            # First, check if a record exists for the given date
            check_query = """
            SELECT EXISTS(
                SELECT 1 FROM opportunity_summary
                WHERE item_code_x = %s AND date = %s
            )
            """
            cur.execute(check_query, (row['item_code_x'], row['Date']))
            exists = cur.fetchone()[0]

            if exists:
                # Update the existing record
                update_query = """
                UPDATE opportunity_summary
                SET count_of_open_opportunity = %s, date_time = %s
                WHERE item_code_x = %s AND date = %s
                """
                cur.execute(update_query, (row['Count Of Open Opportunity'], row['Date Time'], row['item_code_x'], row['Date']))
            else:
                # Insert a new record
                insert_query = """
                INSERT INTO opportunity_summary (item_code_x, count_of_open_opportunity, date, date_time)
                VALUES (%s, %s, %s, %s)
                """
                cur.execute(insert_query, (row['item_code_x'], row['Count Of Open Opportunity'], row['Date'], row['Date Time']))

        conn.commit()
        print("Data inserted/updated successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
    print("Halfway !!")

#222222222222222222222222222222222222222222222222
# %%

    # %%
    import pandas as pd
    from sqlalchemy import create_engine

    # Define your connection parameters
    engine = create_engine('postgresql+psycopg2://postgres:admin%40123@localhost:5432/postgres')

    # SQL to fetch data
    query = "SELECT * FROM product_groups_2"
    key_df = pd.read_sql(query, engine)

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Opportunity'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","deal_pipeline","items.item_code","items.item_group","items.item_name","customer_name"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
            'filters': '[["status", "not in", ["Order Lost","Order Won","Converted","Closed","Lost"]]]'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        opp_df = pd.json_normalize(all_data)
        opp_df.rename(columns={
            'name': 'ID',
            'deal_pipeline': 'Deal Pipeline',
            'item_code': 'Item Code (Opportunity Item)',
            'item_group': 'Item Group (Opportunity Item)',
            'item_name': 'Item Name (Opportunity Item)',
            'customer_name': 'Customer Name'
        }, inplace=True)

        print('fields are correct')
    else:
        columns = ["ID", "Deal Pipeline", "Item Code (Opportunity Item)", "Item Group (Opportunity Item)", "Item Name (Opportunity Item)", "Customer Name"]
        opp_df = pd.DataFrame(columns=columns)


    # %%
    opp_df

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Serial No'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","customer_instrument_id","item_code","item_name","customer","customer_name","territory","serial_no","amc_expiry_date","item_group"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
            # 'filters': '[["item_group", "in", ["Tablet Dissolution Tester - Machine", "Tablet Hardness Tester - Machine"]]]'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        serial_df = pd.json_normalize(all_data)
        serial_df.rename(columns={
            'name': 'ID',
            'customer_instrument_id': 'Customer Instrument ID',
            'item_code': 'Item Code',
            'item_name': 'Item Name',
            'customer': 'Customer',
            'customer_name': 'Customer Name',
            'territory': 'Territory',
            'serial_no': 'Serial No',
            'amc_expiry_date': 'AMC Expiry Date',
            'item_group': 'Item Group'
        }, inplace=True)
        print('fields are correct')
    else:
        columns = ["name","customer_instrument_id","item_code","item_name","customer","customer_name","territory","serial_no","amc_expiry_date","item_group"]
        serial_df = pd.DataFrame(columns=columns)

    # %%
    serial_df

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Sales Order'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","sales_team.sales_person","customer","customer_name","items.item_code","items.item_group","items.item_name","items.qty"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        sales_df = pd.json_normalize(all_data)
        sales_df.rename(columns={
            'name': 'ID',
            'sales_person': 'Sales Person (Sales Team)',
            'customer': 'Customer',
            'customer_name': 'Customer Name',
            'item_code': 'Item Code (Sales Order Item)',
            'item_group': 'Item Group (Sales Order Item)',
            'item_name': 'Item Name (Sales Order Item)',
            'qty': 'Quantity (Sales Order Item)'
        }, inplace=True)
        print('fields are correct')
    else:
        columns = ["ID", "Sales Person (Sales Team)", "Customer", "Customer Name", "Item Code (Sales Order Item)", "Item Group (Sales Order Item)", "Item Name (Sales Order Item)", "Quantity (Sales Order Item)"]
        sales_df = pd.DataFrame(columns=columns)


    # %%
    sales_df

    # %%
    import requests
    import pandas as pd
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from datetime import datetime, timedelta

    base_url = 'https://erpv14.electrolabgroup.com/'
    endpoint = 'api/resource/Customer'
    url = base_url + endpoint

    headers = {
        'Authorization': 'token 3ee8d03949516d0:6baa361266cf807'
    }
    limit_start = 0
    limit_page_length = 1000
    all_data = []
    while True:
        params = {
            'fields': '["name","customer_group","territory"]',
            'limit_start': limit_start,
            'limit_page_length': limit_page_length,
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if 'data' in data:
                so_data = data['data']
                if not so_data:
                    break  # No more data to fetch
                all_data.extend(so_data)
                limit_start += limit_page_length
            else:
                break  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            break

    if all_data:
        customer = pd.json_normalize(all_data)
        print('fields are correct')
    else:
        columns = ["name","deal_pipeline","items.item_code","items.item_group","items.item_name","customer_name"]
        customer = pd.DataFrame(columns=columns)

    # %%
    customer

    # %%
    ### Step 1: identify the key item group from serial nomber by comparing it to serial_df

    # %%
    customer.rename(columns = {'name':'Customer'},inplace = True)
    customer.head()

    # %%

    sales_df.head()

    # %%
    serial_df = pd.merge(serial_df, customer,on = 'Customer', how = 'left')

    # %%
    serial_df.rename(columns = {'Territory_y':'Territory'}, inplace = True)
    serial_df.head()

    # %%
    key_df

    # %%

    # Perform an inner join based on the cleaned item_group
    merged_df = pd.merge(key_df, serial_df, left_on='item_group', right_on='Item Group', how='inner')
    merged_df.rename(columns={'customer_group': 'Customer Group'}, inplace=True)
    merged_df.head()

    # %%
    merged_df.shape


    # %%
    grouped_df = merged_df.groupby(['Item Group', 'item_group', 'Customer', 'item_code','Customer Group','Territory']).size().reset_index(name='count_tam')

    # %%
    grouped_df.shape

    # %%
    opp_df.head()

    # %%
    # Group by 'Item Code' and 'Customer Name' and count the occurrences
    grouped_opp_df = opp_df.groupby(['Item Code (Opportunity Item)', 'Customer Name']).size().reset_index(name='count_opp')
    grouped_opp_df.head()

    # %%
    grouped_opp_df.shape

    # %%
    sales_df.head()

    # %%
    # Group by 'Item Code (Sales Order Item)' and 'Customer Name', then sum the 'Quantity' for each group
    grouped_sales_df = sales_df.groupby(['Item Code (Sales Order Item)', 'Customer Name'])['Quantity (Sales Order Item)'].sum().reset_index(name='count_sales')

    grouped_sales_df.head()

    # %%
    grouped_df['item_code'] = grouped_df['item_code'].astype('str')
    grouped_sales_df['Item Code (Sales Order Item)'] = grouped_sales_df['Item Code (Sales Order Item)'].astype('str')
    grouped_opp_df['Item Code (Opportunity Item)'] = grouped_opp_df['Item Code (Opportunity Item)'].astype('str')

    grouped_df.head()

    # %%
    # First merge grouped_df with grouped_sales_df on item_code and Customer Name
    merged_df_sales = pd.merge(grouped_df, grouped_sales_df, 
                            left_on=['item_code', 'Customer'], 
                            right_on=['Item Code (Sales Order Item)', 'Customer Name'], 
                            how='left')

    # Now merge the result with grouped_opp_df on item_code and Customer Name
    final_merged_df = pd.merge(merged_df_sales, grouped_opp_df, 
                            left_on=['item_code', 'Customer Name'], 
                            right_on=['Item Code (Opportunity Item)', 'Customer Name'], 
                            how='left')

    # Display the result
    final_merged_df.head()

    # %%
    final_merged_df.head()

    # %%
    # Grouping by the desired columns and summing the count columns
    grouped_final_df = final_merged_df.groupby(['Item Group', 'item_group', 'Customer','Customer Group','Territory' ,'item_code']).agg(
        {
            'count_tam': 'sum',   # Summing count_tam
            'count_sales': 'sum',       # Summing count from grouped_sales_df
            'count_opp': 'sum'    # Summing count_opp from grouped_opp_df
        }
    ).reset_index()

    # Display the grouped dataframe
    grouped_final_df.head()

    # %%
    # Selecting only the necessary columns
    selected_df = grouped_final_df[['Item Group', 'item_group', 'Customer', 'item_code','Customer Group', 'Territory', 'count_tam', 'count_sales', 'count_opp']]

    # First, handle count_tam for unique 'Customer', 'Customer Group', 'Territory', and 'Item Group' combinations
    tam_df = selected_df.drop_duplicates(subset=['Customer', 'Customer Group', 'Territory', 'Item Group'])

    # Group by 'Customer', 'Customer Group', 'Territory' to sum count_tam based on unique item groups
    tam_grouped_df = tam_df.groupby(['Customer', 'Customer Group', 'Territory']).agg(
        {'count_tam': 'sum'}  # Summing count_tam for unique Item Groups
    ).reset_index()



    # %%
    # Step 1: Handle item_group separately
    item_group_df = selected_df.groupby(['Customer', 'Customer Group', 'Territory']).agg(
        {
            'item_group': lambda x: ', '.join(x.unique())  # Concatenating unique item_group values
        }
    ).reset_index()

    # Step 2: Drop duplicates based on the subset of columns
    selected_df = selected_df.drop_duplicates(subset=['Customer', 'Customer Group', 'Territory', 'item_code'])

    # Step 3: Handle count_sales and count_opp aggregation
    sales_opp_grouped_df = selected_df.groupby(['Customer', 'Customer Group', 'Territory']).agg(
        {
            'count_sales': 'sum',  # Summing count_sales for all rows
            'count_opp': 'sum',    # Summing count_opp for all rows
        }
    ).reset_index()

    # Step 4: Merge item_group back into the grouped DataFrame
    final_df = sales_opp_grouped_df.merge(item_group_df, on=['Customer', 'Customer Group', 'Territory'], how='left')

    # %%
    final_df.head()

    # %%
    # Merging both dataframes on 'Customer', 'Customer Group', and 'Territory'
    final_grouped_df = pd.merge(tam_grouped_df, final_df, on=['Customer', 'Customer Group', 'Territory'])

    # Display the final grouped dataframe
    final_grouped_df.head()

    import psycopg2

    # Database connection
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='admin@123'
    )
    cursor = conn.cursor()

    # Create table query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS final_merged_machine (
        Customer VARCHAR(255),
        Customer_Group VARCHAR(255),
        Territory VARCHAR(255),
        count_tam INTEGER,
        count_sales INTEGER,
        count_opp INTEGER,
        item_group VARCHAR(555)
    );
    """

    # Execute the create table query
    cursor.execute(create_table_query)
    conn.commit()


    import pandas as pd

    truncate_query = "TRUNCATE TABLE final_merged_machine;"
    cursor.execute(truncate_query)
    for index, row in final_grouped_df.iterrows():
        insert_query = """
        INSERT INTO final_merged_machine (Customer, Customer_Group, Territory, count_tam, count_sales, count_opp, item_group) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, tuple(row))

    # Commit changes
    conn.commit()

    # Close cursor and connection
    cursor.close()
    conn.close()



    # %%
    # Rename the count columns in the final_grouped_df
    final_grouped_df.rename(columns={
        'count_tam': 'Tam Count (Total Quantity Available with Customer)',
        'count_sales': 'Count Sales (According to the Quantity Booked)',
        'count_opp': 'Count Of Open Opportunity',
        'item_group':'Item Group'
    }, inplace=True)



    df = final_grouped_df[['Count Of Open Opportunity']].copy()
    df['Total Count'] = 1
    df = df.groupby('Total Count', as_index=False).agg({'Count Of Open Opportunity': 'sum' })
    df['Date'] = datetime.now().date()
    df['Date Time'] = datetime.now()

    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='admin@123'
        )
        cur = conn.cursor()

        # Create the new table if it doesn't already exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS opportunity_summary_new (
            total_count INTEGER,
            count_of_open_opportunity INTEGER,
            date DATE UNIQUE,
            date_time TIMESTAMP
        )
        """
        cur.execute(create_table_query)
        conn.commit()

        # Loop through each row in df to insert or update data
        for index, row in df.iterrows():
            # Check if a record exists for the given date
            check_query = """
            SELECT EXISTS(
                SELECT 1 FROM opportunity_summary_new
                WHERE date = %s
            )
            """
            cur.execute(check_query, (row['Date'],))
            exists = cur.fetchone()[0]

            if exists:
                # Update the existing record
                update_query = """
                UPDATE opportunity_summary_new
                SET total_count = %s, count_of_open_opportunity = %s, date_time = %s
                WHERE date = %s
                """
                cur.execute(update_query, (row['Total Count'], row['Count Of Open Opportunity'], row['Date Time'], row['Date']))
            else:
                # Insert a new record
                insert_query = """
                INSERT INTO opportunity_summary_new (total_count, count_of_open_opportunity, date, date_time)
                VALUES (%s, %s, %s, %s)
                """
                cur.execute(insert_query, (row['Total Count'], row['Count Of Open Opportunity'], row['Date'], row['Date Time']))

        conn.commit()
        print("Data inserted/updated successfully in opportunity_summary_new!")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"DONE !! at {current_time}")

except Exception as e:
    print(f"An error occurred while generating Excel: {e}")


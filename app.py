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
EXCEL_FILE_PATH = 'HelpThemToBuy.xlsx'
import plotly.express as px

@app.route('/nonmachine')
def index2():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='admin@123'
        )
        
        query = "SELECT total_count, count_of_open_opportunity, date FROM opportunity_summary_new"
        df = pd.read_sql(query, conn)
        
        machine_data_query = "SELECT * FROM final_merged_machine"
        machine_df = pd.read_sql(machine_data_query, conn)
        machine_df.rename(columns={
            'item_group': 'Item Group',
            'customer': 'Customer',
            'customer_group': 'Customer Group',
            'territory': 'Territory',
            'count_tam': 'Tam Count (Total Quantity Available with Customer)',
            'count_sales': 'Count Sales (According to the Quantity Booked)',
            'count_opp': 'Count Of Open Opportunity'
        }, inplace=True)
        conn.close()
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date')

        fig = px.line(df, x='date', y='count_of_open_opportunity', title='Total Open Opportunities Over Time')

        fig.update_traces(
            mode="lines+markers+text",
            marker=dict(size=8, color='black'),  
            line=dict(color='darkblue', width=2),
            text=[f"{val}" for val in df['count_of_open_opportunity']],
            textposition="top center"
        )

        fig.update_layout(
            title='Total Open Opportunities Over Time',
            xaxis_title='Date',
            yaxis_title='Count of Open Opportunity',
            xaxis=dict(showgrid=True, gridcolor='lightgray'),
            yaxis=dict(showgrid=True, gridcolor='lightgray'),
            width=1700,  
            height=500 
        )

        graph_html = fig.to_html(full_html=False)
                
        machine_table_html = machine_df.to_html(classes='table table-striped', index=False)
        
        return render_template('last.html', machine_table_html=machine_table_html, graph_html=graph_html)
    except Exception as e:
        print("Error:", e)
        return "An error occurred: " + str(e)


import plotly.graph_objects as go
import psycopg2
import pandas as pd
from flask import Flask, render_template

@app.route('/')
def index():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='postgres',
        user='postgres',
        password='admin@123'
    )

    final_query = "SELECT item_group_x, customer, item_code_x, customer_group, territory, count_tam, count_sales, count_opp FROM final_merged_data"
    final_merged_df = pd.read_sql(final_query, conn)

    final_merged_df.rename(columns={
        'item_group_x': 'Item Group',
        'customer': 'Customer',
        'item_code_x': 'Item Code',
        'customer_group': 'Customer Group',
        'territory': 'Territory',
        'count_tam': 'Tam Count (Total Quantity Available with Customer)',
        'count_sales': 'Count Sales (According to the Quantity Booked)',
        'count_opp': 'Count Of Open Opportunity'
    }, inplace=True)

    sheets_html = {}
    for item_code in final_merged_df['Item Code'].unique():
        df_item_code = final_merged_df[final_merged_df['Item Code'] == item_code]
        df_item_code = df_item_code.drop(columns=['Item Code'])
        sheets_html[item_code] = df_item_code.to_html(classes='table table-striped custom-table', index=False)

    query = "SELECT item_code_x, count_of_open_opportunity, date FROM opportunity_summary"
    df = pd.read_sql(query, conn)
    
    df['item_code_x'] = df['item_code_x'].str.strip()

    unique_item_codes = df['item_code_x'].unique()
    graphs_html = {}
    for item_code in unique_item_codes:
        df_item_code = df[df['item_code_x'] == item_code].copy()

        df_item_code['date'] = pd.to_datetime(df_item_code['date'])
        df_item_code = df_item_code.set_index('date').sort_index()

        all_dates = pd.date_range(start=df_item_code.index.min(), end=df_item_code.index.max())

        df_item_code = df_item_code.reindex(all_dates)
        df_item_code['count_of_open_opportunity'] = df_item_code['count_of_open_opportunity'].astype(float)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_item_code.index,
            y=df_item_code['count_of_open_opportunity'],
            mode='lines+markers+text',
            marker=dict(color='blue'),
            name='Open Opportunities',
            text=[f"{val}" if not pd.isna(val) else "" for val in df_item_code['count_of_open_opportunity']],
            textposition="top center",
            connectgaps=True
        ))

        fig.update_layout(
            title=f'Opportunities for {item_code} Over Time',
            xaxis_title='Date',
            yaxis_title='Count of Open Opportunity',
            xaxis=dict(tickformat="%Y-%m-%d", tickangle=0),
            width=1700,
            height=500 
        )

        graphs_html[item_code] = f'<div class="plotly-graph" style="max-width: 1700px;">{fig.to_html(full_html=False, include_plotlyjs="cdn")}</div>'

    conn.close()
    
    return render_template('index.html', sheets_html=sheets_html, graphs_html=graphs_html)


from flask import send_file, jsonify

import os
import pandas as pd
import psycopg2
from flask import Flask, send_file, jsonify

@app.route('/download')
def download():
    # Get the user's Downloads folder
    # download_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    download_file_path = os.path.join( 'Spares_Product_Segmentation.xlsx')

    # Check if the file already exists and delete it
    if os.path.exists(download_file_path):
        os.remove(download_file_path)

    try:
        # Create the Excel file with the data
        with pd.ExcelWriter(download_file_path, engine='xlsxwriter') as writer:
            # Connect to the database
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='postgres',
                user='postgres',
                password='admin@123'
            )

            # Fetch all the data from the database to write to the Excel file
            query = "SELECT * FROM final_merged_data"  # Your actual table name
            final_merged_df = pd.read_sql(query, conn)

            # Check if DataFrame is not empty
            if final_merged_df.empty:
                return jsonify({"error": "No data found in the table."}), 404

            # Rename columns before saving
            final_merged_df.rename(columns={
                'item_group_x': 'Item Group',
                'customer': 'Customer',
                'item_code_x': 'Item Code',
                'customer_group': 'Customer Group',
                'territory': 'Territory',
                'count_tam': 'Tam Count (Total Quantity Available with Customer)',
                'count_sales': 'Count Sales (According to the Quantity Booked)',
                'count_opp': 'Count Of Open Opportunity'
            }, inplace=True)

            # Create a format for the header
            format_header = writer.book.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#4F81BD', 'border': 1})
            format_cell = writer.book.add_format({'border': 1})

            for item_code in final_merged_df['Item Code'].unique():  # Ensure you reference the renamed column
                df_item_code = final_merged_df[final_merged_df['Item Code'] == item_code]
                
                # Write to Excel with formatting
                df_item_code.to_excel(writer, sheet_name=str(item_code), index=False)

                # Get the worksheet and apply formatting
                worksheet = writer.sheets[str(item_code)]

                # Apply the header format
                for col_num, value in enumerate(df_item_code.columns.values):
                    worksheet.write(0, col_num, value, format_header)  # Write headers with formatting

                # Apply cell formatting for data rows
                for row_num in range(1, len(df_item_code) + 1):
                    for col_num in range(len(df_item_code.columns)):
                        cell_value = df_item_code.iloc[row_num - 1, col_num]
                        if pd.isna(cell_value):
                            worksheet.write(row_num, col_num, '', format_cell)  # Write empty string for NaN
                        else:
                            worksheet.write(row_num, col_num, cell_value, format_cell)

                # Set column widths
                for col_num in range(len(df_item_code.columns)):
                    max_len = df_item_code[df_item_code.columns[col_num]].astype(str).map(len).max()
                    worksheet.set_column(col_num, col_num, max_len + 2)

            # Close the database connection
            conn.close()

        # Send the file for download
        return send_file(download_file_path, as_attachment=True)

    except Exception as e:
        # Log the error
        print(f"Error occurred: {e}")
        return jsonify({"error": str(e)}), 500


def create_excel_file(df, file_name):
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')

        workbook  = writer.book
        worksheet = writer.sheets['Data']

        # Define your header format
        format_header = workbook.add_format({
            'bold': True,
            'font_color': 'white',
            'bg_color': '#4F81BD',
            'border': 1
        })

        # Apply the header format
        for col_num, value in enumerate(df.columns):
            worksheet.write(0, col_num, value, format_header)

        # Auto-adjust column widths
        for i, col in enumerate(df.columns):
            max_length = max(df[col].astype(str).map(len).max(), len(col))  # Get the max length of the column
            worksheet.set_column(i, i, max_length + 2) 

@app.route('/download2')
def download_file2():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='postgres',
            user='postgres',
            password='admin@123'
        )

        # Query data from the final_merged_machine table
        query = "SELECT * FROM final_merged_machine"
        df = pd.read_sql(query, conn)
        conn.close()  # Close the connection after fetching data

        df.rename(columns={
                'item_group': 'Item Group',
                'customer': 'Customer',
                'customer_group': 'Customer Group',
                'territory': 'Territory',
                'count_tam': 'Tam Count (Total Quantity Available with Customer)',
                'count_sales': 'Count Sales (According to the Quantity Booked)',
                'count_opp': 'Count Of Open Opportunity'
            }, inplace=True)

        # Define file name for the download (this will appear as the download name)
        file_name = 'Machine_Product_Segmentation.xlsx'

        # Create the Excel file with formatting
        create_excel_file(df, file_name)


        # Send the file to the client for download
        return send_file(file_name, as_attachment=True)

    except Exception as e:
        print("Error:", e)
        return "An error occurred: " + str(e)

if __name__ == '__main__':
    app.run(debug=False)

import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError 
from datetime import datetime, timedelta
from streamlit import session_state as state


# PostgreSQL Database connection setup (replace with actual credentials)
DATABASE_URL = "postgresql://postgres:Divyanshu%4099@127.0.0.1:5432/clinic_db"
engine = create_engine(DATABASE_URL)


# Function to initialize user table in PostgreSQL
def create_user_table():
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) NOT NULL,
                    password VARCHAR(50) NOT NULL,
                    role VARCHAR(50) NOT NULL,
		    practitioner_id INTEGER UNIQUE NOT NULL,
                    active BOOLEAN DEFAULT TRUE
                );
            """))
            conn.commit()  # Commit the transaction to ensure changes are applied
        print("Table 'users' created successfully (if it did not exist).")
    except Exception as e:
        print(f"Error creating 'users' table: {e}")

#ALTER TABLE users ADD COLUMN practitioner_id VARCHAR(50);

# Ensure table creation before any queries
def initialize_app():
    create_user_table()  # Create the users table if it doesn't exist

# Initialize the application (create the table)
initialize_app()

# Function to insert a new user into the database
def insert_user(username, password, role, practitioner_id):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO users (username, password, role, practitioner_id, active)
                VALUES (:username, :password, :role, :practitioner_id, TRUE)
            """), {"username": username, "password": password, "role": role, "practitioner_id": practitioner_id})
            conn.commit()
        print(f"User '{username}' inserted successfully.")
    except Exception as e:
        print(f"Error inserting user '{username}': {e}")

# Example: Insert the master user
insert_user('master_user', 'masterpass', 'master', '0000')

# Function to fetch users from the database
#def fetch_users_from_db():
#    with engine.connect() as conn:
#        result = conn.execute(text("SELECT username, password, role, practitioner_id FROM users"))
#        users = {row['username']: {'password': row['password'], 'role': row['role'], 'practitioner_id': row['practitioner_id']} for row in result}
#    return users

from sqlalchemy import text

def fetch_users_from_db():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT username, password, role, practitioner_id FROM users"))
        users = {}
        rows = result.fetchall()
        if rows:
            users = {row[0]: {'password': row[1], 'role': row[2], 'practitioner_id': row[3]} for row in rows}
            print(users)  # Debugging step to check the fetched data
        else:
            print("No users found in the database.")
        return users


# Now you can safely fetch users from the database
#user_db = fetch_users_from_db()

# Function to add a new user
#def add_user(username, password, role):
#    with engine.connect() as conn:
#        conn.execute(f"INSERT INTO users (username, password, role) VALUES ('{username}', '{password}', #'{role}')")

# Function to add a new user to the database
def add_user(username, password, role, practitioner_id):
    try:
        with engine.connect() as conn:
            # Use parameterized queries to safely insert the user data
            query = text("INSERT INTO users (username, password, role, practitioner_id) VALUES (:username, :password, :role, :practitioner_id)")
            conn.execute(query, {"username": username, "password": password, "role": role, "practitioner_id": practitioner_id})
            conn.commit()
            st.success(f"User {username} added successfully.")
    except IntegrityError as e:  # Catch the unique constraint violation
            # Check if the error is due to the practitioner_id or username already existing
            if "duplicate key value violates unique constraint" in str(e):
                st.error(f"User with Practitioner ID {practitioner_id} or Username {username} already exists.")
            else:
                st.error(f"An error occurred: {e}")
    except Exception as e: 
            st.error(f"An unexpected error occurred: {e}")

# Function to update user role or password
def update_user(practitioner_id, new_password=None, new_role=None):
    with engine.connect() as conn:
        if new_password:
            conn.execute(f"UPDATE users SET password = '{new_password}' WHERE practitioner_id = '{practitioner_id}'")
        if new_role:
            conn.execute(f"UPDATE users SET role = '{new_role}' WHERE practitioner_id = '{practitioner_id}'")

# Function to delete a user
def delete_user(username):
    with engine.connect() as conn:
        conn.execute(f"DELETE FROM users WHERE practitioner_id = '{practitioner_id}'")

# Function to check session timeout
def check_session_timeout():
    if 'last_active' not in state:
        state['last_active'] = datetime.now()
    else:
        if datetime.now() - state['last_active'] > timedelta(minutes=15):  # Timeout after 15 minutes of inactivity
            st.warning("Session timed out due to inactivity.")
            state.clear()
            st.experimental_rerun()

# Sample data for performance and target (replace with actual data and methods)
PERFORMANCE_FILE = r"C:\Users\divfi\Desktop\Sem4 DA\Capstone\Re_ Dashboard project Langara\clinic_performance.xlsx"
TARGET_FILE = r"C:\Users\divfi\Desktop\Sem4 DA\Capstone\Re_ Dashboard project Langara\clinic_target.xlsx"

# Function to load data (replace with real data loading method)
def load_data(file_path):
    return pd.read_excel(file_path, engine='openpyxl')

# Load performance and target data
performance_df = load_data(PERFORMANCE_FILE)
target_df = load_data(TARGET_FILE)

# Convert 'Appt. Date' to datetime
performance_df['Appt. Date'] = pd.to_datetime(performance_df['Appt. Date'], format='%m/%d/%Y')

# Extract week and month from 'Appt. Date'
performance_df['Week'] = performance_df['Appt. Date'].dt.isocalendar().week
performance_df['Month'] = performance_df['Appt. Date'].dt.month
performance_df['Year'] = performance_df['Appt. Date'].dt.year

# Aggregate performance data (weekly and monthly) by practitioner
def aggregate_performance(performance_df, target_df):
    weekly_performance = performance_df.groupby(['Week', 'Practitioner ID']).agg(
        Total_Hours=('Total Hours', 'sum'),
        Total_Income=('Total Income', 'sum')
    ).reset_index()


    # Merge with target data for weekly and monthly comparison
    weekly_data = pd.merge(weekly_performance, target_df[['Week', 'Practitioner ID', 'Target Hour']], on=['Week', 'Practitioner ID'], how='left')
#    monthly_data = pd.merge(monthly_performance, target_df, on='Month', how='left')
    
    # Calculate performance ratio
    weekly_data['Performance_Ratio'] = weekly_data['Total_Hours'] / weekly_data['Target Hour']
#    monthly_data['Performance_Ratio'] = monthly_data['Total_Hours'] / monthly_data['Target Hour']
    
    return weekly_data #, monthly_data

# Display performance analytics
def display_performance_analytics(weekly_performance):#, monthly_data):
    # Weekly performance chart (bar chart comparing actual vs. target hours)
    st.subheader("Weekly Performance")
    weekly_chart = px.bar(
        weekly_performance,
        x='Week',
        y=['Total_Hours', 'Target Hour'],
        color='Practitioner ID',
        barmode='group',
        title="Weekly Hours Worked vs Target",
        labels={'value': 'Hours', 'variable': 'Legend'}
    )
    st.plotly_chart(weekly_chart)


    # Performance ratio comparison
    st.subheader("Performance Ratio (Hours Worked / Target Hours)")
    performance_ratio_chart = px.bar(
        weekly_performance,
        x='Practitioner ID',
        y='Performance_Ratio',
        color='Practitioner ID',
        title="Performance Ratio (Weekly)",
        labels={'Performance_Ratio': 'Performance Ratio'}
    )
    st.plotly_chart(performance_ratio_chart)

# Master dashboard for clinic performance
def master_dashboard(performance_df, target_df):
    weekly_data = aggregate_performance(performance_df, target_df)
    display_performance_analytics(weekly_data)#, monthly_data)
    # Group performance data by week and practitioner
    weekly_performance = performance_df.groupby(['Week', 'Practitioner ID']).agg(
        Total_Hours=('Total Hours', 'sum'),
        Total_Income=('Total Income', 'sum')
    ).reset_index()

    print(weekly_performance.columns)

    # Merge with target data to get target values
    combined_df = pd.merge(weekly_performance, target_df, on=['Week', 'Practitioner ID'], how='left')
    print(combined_df.columns)

    # Calculate ratio performance
    combined_df['Ratio_Performance'] = combined_df['Total_Hours'] / combined_df['Target Hour']

    # Display weekly performance
    st.subheader("Weekly Performance")
    st.dataframe(combined_df)

    # Monthly View (Assuming 4 weeks for each month)
    monthly_performance = combined_df.groupby(['Practitioner ID']).agg(
        Total_Hours=('Total_Hours', 'sum'),
        Total_Income=('Total_Income', 'sum'),
        Target_Hours=('Target Hour', 'sum'),
        Target_Income=('Target Income', 'sum')
    ).reset_index()

    monthly_performance['Ratio_Performance'] = monthly_performance['Total_Hours'] / monthly_performance['Target_Hours']

    # Display monthly performance
    st.subheader("Monthly Performance")
    st.dataframe(monthly_performance)

# Function to filter data by user role (master or specific practitioner)
def get_user_data(df, user_role, practitioner_id=None):
    if user_role == 'master':
        return df
    elif user_role == 'secondary' and practitioner_id:
        return df[df['Practitioner ID'] == practitioner_id]

# Login function (now using database)
def login(username, password, user_db):
    if username in user_db and user_db[username]['password'] == password:
        state['role'] = user_db[username]['role']
        state['username'] = username
        state['practitioner_id'] = user_db[username]['practitioner_id']  # Store practitioner_id
        state['last_active'] = datetime.now()
        return user_db[username]['role']
    return None

# User Management for Master Users (CRUD operations)
def manage_users():
    st.subheader("User Management")
    
    # Add new user
    st.write("### Add New User")
    new_username = st.text_input("New Username")
    new_password = st.text_input("New Password", type="password")
    new_role = st.selectbox("Role", ['master', 'secondary'])
    new_practitioner_id = st.text_input("Practitioner ID")
    if st.button("Add User"):
        add_user(new_username, new_password, new_role, new_practitioner_id)
        #st.success(f"User {new_username} added successfully.")
    
    # Update existing user
    st.write("### Update Existing User")
    update_username = st.text_input("User to Update")
    new_role_update = st.selectbox("New Role", ['master', 'secondary'], key='update_role')
    new_password_update = st.text_input("New Password (leave blank if not changing)", type="password", key='update_password')
    if st.button("Update User"):
        update_user(update_username, new_password=new_password_update, new_role=new_role_update)
        st.success(f"User {update_username} updated successfully.")
    
    # Delete user
    st.write("### Delete User")
    delete_username = st.text_input("Username to Delete")
    if st.button("Delete User"):
        delete_user(delete_username)
        st.success(f"User {delete_username} deleted successfully.")

def load_target_data():
    return pd.read_excel(TARGET_FILE)

# Function to save the updated target data back to the file
def save_target_data(df):
    df.to_excel(TARGET_FILE, index=False)

# Load the target data
target_df = load_target_data()

# Function to check if Practitioner ID and Appointment Type already exist
def check_appointment_exists(practitioner_id, week, appt_type):
    existing_record = target_df[
        (target_df['Practitioner ID'] == practitioner_id) &
        (target_df['Week'] == week) &
        (target_df['Appt. Type'] == appt_type)
    ]
    return not existing_record.empty


def update_target(practitioner_id, week, appt_type, new_target_income, new_target_hours):
    try:
        # Check if the practitioner ID and appointment type already exist
        if check_appointment_exists(practitioner_id, week, appt_type):
            target_df.loc[
                (target_df['Practitioner ID'] == practitioner_id) & 
                (target_df['Week'] == week) & 
                (target_df['Appt. Type'] == appt_type), 
                ['Target Income', 'Target Hour']
            ] = [new_target_income, new_target_hours]
            
            # Save the updated target data back to the file
            save_target_data(target_df)
            st.success(f"Target updated for Practitioner ID {practitioner_id}, Week {week}, and Appointment Type {appt_type}.")
        else:
            st.error("The Practitioner ID and Appointment Type combination does not exist. Please enter a valid record.")
    except Exception as e:
        st.error(f"Error updating the target: {e}")

# Function to allow master user to update the target in the UI
def master_update_target():
    st.subheader("Update Target Income and Hours for Practitioner")
    
    # Input fields for updating target data with unique keys
    practitioner_id = st.number_input("Practitioner ID", min_value=1, step=1, key="practitioner_id_input")
    week = st.number_input("Week", min_value=1, max_value=52, step=1, key="week_input")
    appt_type = st.text_input("Appointment Type", key="appt_type_input").strip()
    
    new_target_income = st.number_input("New Target Income", min_value=0, step=100, key="new_target_income_input")
    new_target_hours = st.number_input("New Target Hours", min_value=0, step=1, key="new_target_hours_input")
    
    if st.button("Update Target", key="update_target_button"):
        # Validate inputs: Practitioner ID must be > 0, Appointment Type must be valid (not empty and only letters)
        if practitioner_id > 0 and appt_type and appt_type.isalpha():
            update_target(practitioner_id, week, appt_type, new_target_income, new_target_hours)
        else:
            st.error("Please enter a valid Practitioner ID (number) and a valid Appointment Type (letters only).")

# Streamlit app layout
st.title("Springboard Clinic - Performance Dashboard")

# If session exists, check for timeout
if 'role' in state:
    check_session_timeout()

# If no session is active, ask for login
if 'role' not in state:
    # Create the user table if not exists
    create_user_table()

    # Fetch user data from the database
    user_db = fetch_users_from_db()

    # Input fields for username and password
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        role = login(username, password, user_db)
        if role:
            st.success(f"Welcome, {username}! You are logged in as a {role} user.")
        else:
            st.error("Invalid username or password.")
else:
    # If the user is logged in as 'master', show master dashboard and user management
    if state['role'] == 'master':
        st.subheader(f"Welcome, {state['username']} (Master User)")
        manage_users()
        master_dashboard(performance_df, target_df)
        master_update_target()
# If the user is a 'secondary' user, show personalized dashboard
    elif state['role'] == 'secondary':
        try:
            st.subheader(f"Welcome {state['username']}! This is your performance dashboard.")
    
        # Get the practitioner's ID from session state
            practitioner_id = state.get('practitioner_id')  # Fetch from session
            st.write(f"Practitioner ID: {practitioner_id}")

        # Clean up column names to remove extra spaces or mismatches
            performance_df.columns = performance_df.columns.str.strip()
            target_df.columns = target_df.columns.str.strip()

        # Step 1: Print available Practitioner IDs in performance_df and target_df for debugging
            st.write(f"Practitioner IDs available in performance data: {performance_df['Practitioner ID'].unique()}")
            st.write(f"Practitioner IDs available in target data: {target_df['Practitioner ID'].unique()}")
    
        # Ensure the practitioner_id is an integer for matching in the DataFrame
            practitioner_id = int(practitioner_id)
    
        # Step 2: Filter data for the logged-in user based on Practitioner ID
            user_performance = performance_df[performance_df['Practitioner ID'] == practitioner_id]
            user_target = target_df[target_df['Practitioner ID'] == practitioner_id]

        # Step 3: Display the filtered user data for debugging
            st.subheader("Filtered Data for Your Performance:")
            st.dataframe(user_performance)

        # Step 4: Check if data is available
            if user_performance.empty:
                st.warning("No performance data available for this user.")
            else:
            # Group by Week and sum Total Hours for weekly performance
                weekly_performance = user_performance.groupby(['Week', 'Practitioner ID', 'Appt. Type']).agg(
                    Total_Hours=('Total Hours', 'sum'),
                    Total_Income=('Total Income', 'sum')
                ).reset_index()
            # Debugging: Print the columns of weekly_performance
                st.write("Columns in weekly_performance:", weekly_performance[:5:])
                st.write("Columns in weekly_performance:", user_target[:5:])

            # Check if 'Week' and 'Practitioner ID' columns exist before merging
                if 'Week' in weekly_performance.columns and 'Practitioner ID' in user_target.columns:
                # Merge weekly performance with target data
                    combined_df = pd.merge( weekly_performance, user_target, on=['Practitioner ID','Week'], how='left')

                # Calculate the performance ratio (Total Hours Worked / Target Hours)
                    combined_df['Performance_Ratio'] = combined_df['Total_Hours'] / combined_df['Target Hour']

                    # Debug: Check the Performance_Ratio before plotting
                    st.write("Performance Ratios:", combined_df[['Week', 'Performance_Ratio']])
                    st.write("Combined DataFrame columns:", combined_df.columns)

                    # Checking for missing data
                    st.write("Missing data in each column:")
                    st.write(combined_df.isnull().sum())
                    


   # Chart 1: Weekly Total Hours Worked vs. Target Hours by Appointment Type
                    st.subheader("Weekly Total Hours Worked v/s Target Hours by Appointment Type")
# Merge the weekly performance with the target data on Practitioner ID and Week
                    combined_hi_df = pd.merge(weekly_performance, user_target, on=['Week', 'Practitioner ID', 'Appt. Type'], how='left')
                    combined_hours_df = pd.melt(
                        combined_hi_df, 
                        id_vars=['Week', 'Appt. Type'], 
                        value_vars=['Total_Hours', 'Target Hour'], 
                        var_name='Hours Type', 
                        value_name='Hours'
                     )
# Plot the hours worked vs target hours by appointment type using a line chart
                    try:
                        fig_hours_line = px.line(
                            combined_hours_df,
                            x='Week',
                            y='Hours',
                            color='Appt. Type',
                            line_dash='Hours Type',
                            #symbol='Hours Type',
                            title="Weekly Total Hours Worked vs Target Hours",
                            labels={'Hours': 'Hours Worked/Target Hours', 'Week': 'Week Number'}
                        )
                        st.plotly_chart(fig_hours_line)
                    except ValueError as e:
                        st.error(f"ValueError encountered: {e}")

# Chart 2: Weekly Total Income Earned vs. Target Income by Appointment Type
                    st.subheader("Weekly Total Income Earned vs Target Income by Appointment Type")
# Fill missing values (optional, depending on your data)
                    combined_income_df = pd.melt(
                        combined_hi_df, 
                        id_vars=['Week', 'Appt. Type'], 
                        value_vars=['Total_Income', 'Target Income'], 
                        var_name='Income Type', 
                        value_name='Income'
                     )
# Plot the hours worked vs target hours by appointment type using a line chart
                    try:
                        fig_income_line = px.line(
                            combined_income_df,
                            x='Week',
                            y='Income',
                            color='Appt. Type',
                            line_dash='Income Type',
                            symbol='Income Type',
                            title="Weekly Total Income Earned vs Target Income",
                            labels={'Income': 'Income Earned/Target Income', 'Week': 'Week Number'}
                        )
                        st.plotly_chart(fig_income_line)
                    except ValueError as e:
                        st.error(f"ValueError encountered: {e}")                       

    
        except KeyError as e:
            st.error(f"KeyError encountered: {e}. Ensure 'Practitioner ID' and 'Week' columns are correctly named.")
        except ValueError as e:
            st.error(f"ValueError encountered: {e}. Check for any type mismatches or data conversion errors.")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}. Please check the data and code.")

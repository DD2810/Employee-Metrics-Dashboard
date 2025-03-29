import streamlit as st
import pandas as pd
import time
from sqlalchemy import text
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError 
from datetime import datetime, timedelta
from streamlit import session_state as state

# PostgreSQL Database connection setup (credentials)
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

# Insert the master user
insert_user('master_user', 'masterpass', 'master', '0000')

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
def delete_user(practitioner_id):
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
@st.cache_data
def load_data(file_path):
    return pd.read_excel(file_path, engine='openpyxl')

# Load performance and target data
performance_df = load_data(PERFORMANCE_FILE)
target_df = load_data(TARGET_FILE)

# Convert the 'Appt. Date' in performance_df to datetime format
performance_df['Appt. Date'] = pd.to_datetime(performance_df['Appt. Date'], format='%d/%m/%y')

# Extract the week number and year from 'Appt. Date' in performance_df
performance_df['Week'] = performance_df['Appt. Date'].dt.isocalendar().week
performance_df['Month'] = performance_df['Appt. Date'].dt.month
performance_df['Year'] = performance_df['Appt. Date'].dt.year

def master_dashboard(performance_df, target_df):
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

    # If the user is a 'secondary' user, show personalized dashboard
    elif state['role'] == 'secondary':
        st.subheader(f"Welcome {state['username']}! This is your performance dashboard.")
        # Get the practitioner's ID from session state
        practitioner_id = state.get('practitioner_id')  # Fetch from session
        st.write(f"Practitioner ID: {practitioner_id}")
        practitioner_id = st.session_state['practitioner_id']

        # Step 1: Print available Practitioner IDs in performance_df (for debugging)
        st.write(f"Practitioner IDs available in performance data: {performance_df['Practitioner ID'].unique()}")

        # Step 2: Print Practitioner ID from session (for debugging)
        st.write(f"Practitioner ID from session state: {practitioner_id}")

        # Step 1: Print available Practitioner IDs in performance_df (for debugging)
        st.write(f"Practitioner IDs available in target: {target_df['Practitioner ID'].unique()}")

        # Step 1: Filter data for the logged-in user (based on Practitioner Name)
        user_performance = performance_df[performance_df['Practitioner ID'] == practitioner_id]
        user_target = target_df[target_df['Practitioner ID'] == practitioner_id] 

        # Ensure 'Practitioner ID' exists and is named correctly in both dataframes
        performance_df.columns = [col.strip() for col in performance_df.columns]  # Remove any extra spaces
        target_df.columns = [col.strip() for col in target_df.columns]

        st.write("Performance DataFrame columns:", performance_df.columns)
        st.write("Target DataFrame columns:", target_df.columns)

    	# Debugging: Display the filtered user data
        st.subheader("Filtered Data for Your Performance:")
        st.dataframe(user_performance)  # Display the user's performance data

    	# Step 2: Check if there is data for the user, if not show a message
        if user_performance.empty:
        	st.warning("No performance data available for this user.")
        else:
        	# Group by Week and sum Total Hours
                weekly_performance = user_performance.groupby('Week').agg(
                      Total_Hours=('Total Hours', 'sum')
                ).reset_index()

        	# Merge weekly performance with target data (based on practitioner and week)
                                     
                combined_df = pd.merge(weekly_performance[:5], user_target[:5], on=['Week', 'Practitioner ID'], how='left')
                # Ensure 'Practitioner ID' exists and is named correctly in both dataframes
                performance_df.columns = [col.strip() for col in performance_df.columns]  # Remove any extra spaces
                target_df.columns = [col.strip() for col in target_df.columns]

                st.write("Performance DataFrame columns:", performance_df.columns)
                st.write("Target DataFrame columns:", target_df.columns)

# Ensure both dataframes have 'Practitioner ID' as the column
                if 'Practitioner ID' not in performance_df.columns:
                     st.error("The 'Practitioner ID' column is missing from performance_df.")
                if 'Practitioner ID' not in target_df.columns:
                     st.error("The 'Practitioner ID' column is missing from target_df.")

        	# Calculate the performance ratio (Total Hours Worked / Target Hours)
                combined_df['Performance_Ratio'] = combined_df['Total_Hours'] / combined_df['Target Hour']

        	# Add weekly visualization
                st.subheader("Weekly Performance Ratio (Hours Worked / Target Hours)")
                performance_ratio_chart = px.bar(
            	combined_df,
            	x='Week',
            	y='Performance_Ratio',
            	title="Performance Ratio (Weekly)",
            	labels={'Performance_Ratio': 'Performance Ratio'}
        	)
                st.plotly_chart(performance_ratio_chart)
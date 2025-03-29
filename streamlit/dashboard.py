import streamlit as st
import pandas as pd

# Part 1: Performance display (Oct 13: no need to display performance but change to display user management system, add, edit, grant acces to user)

# Excel file paths
PERFORMANCE_FILE = '/Users/andynguyen/Downloads/0 Project/Capstone DANA 4850/clinic_performance.xlsx'
TARGET_FILE = '/Users/andynguyen/Downloads/0 Project/Capstone DANA 4850/clinic_target.xlsx'

# Function to read Excel data
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
performance_df['Year'] = performance_df['Appt. Date'].dt.year

def master_dashboard(performance_df, target_df):
    # Group performance data by week and practitioner
    weekly_performance = performance_df.groupby(['Week', 'Practitioner Name']).agg(
        Total_Hours=('Total Hours', 'sum'),
        Total_Income=('Total Income', 'sum')
    ).reset_index()

    print(weekly_performance.columns)

    # Merge with target data to get target values
    combined_df = pd.merge(weekly_performance, target_df, on=['Week', 'Practitioner Name'], how='left')
    print(combined_df.columns)

    # Calculate ratio performance
    combined_df['Ratio_Performance'] = combined_df['Total_Hours'] / combined_df['Target Hour']

    # Display weekly performance
    st.subheader("Weekly Performance")
    st.dataframe(combined_df)

    # Monthly View (Assuming 4 weeks for each month)
    monthly_performance = combined_df.groupby(['Practitioner Name']).agg(
        Total_Hours=('Total_Hours', 'sum'),
        Total_Income=('Total_Income', 'sum'),
        Target_Hours=('Target Hour', 'sum'),
        Target_Income=('Target Income', 'sum')
    ).reset_index()

    monthly_performance['Ratio_Performance'] = monthly_performance['Total_Hours'] / monthly_performance['Target_Hours']

    # Display monthly performance
    st.subheader("Monthly Performance")
    st.dataframe(monthly_performance)


# Part 2: Log in function
# Sample user database (Oct 13 beinga dvised to save to database Postgre or something else)
user_db = {
    'master_user': {'password': 'masterpass', 'role': 'master'},
    'secondary_user1': {'password': 'userpass1', 'role': 'secondary'},
    'secondary_user2': {'password': 'userpass2', 'role': 'secondary'}
}

# Function to filter data by user role (master or specific practitioner)
def get_user_data(df, user_role, practitioner_name=None):
    if user_role == 'master':
        return df
    elif user_role == 'secondary' and practitioner_name:
        return df[df['Practitioner Name'] == practitioner_name]

# Login function
def login(username, password):
    user = user_db.get(username)
    if user and user['password'] == password:
        st.session_state['role'] = user['role']
        st.session_state['username'] = username
        return user['role']
    return None

# Streamlit app layout
st.title("Springboard Clinic - Performance Dashboard")

# Input fields for username and password
if 'role' not in st.session_state:
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        role = login(username, password)
        if role:
            st.session_state['role'] = role  # Store the role in session state ---> need to store in the database
            st.session_state['username'] = username  # Store the username in session state
            st.success(f"Welcome, {username}! You are logged in as a {role} user.")
        else:
            st.error("Invalid username or password.")
else:
    # If the user is logged in, show the dashboard
    if st.session_state['role'] == 'master':
        master_dashboard(performance_df, target_df)
    else:
        st.write("Secondary user dashboard view will be implemented here.")
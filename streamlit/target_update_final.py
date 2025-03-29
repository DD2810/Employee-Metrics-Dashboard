import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import psycopg2
import matplotlib.pyplot as plt
from streamlit_option_menu import option_menu

host = "cpsc-a3-thaiduonganh1234-06f9.b.aivencloud.com"
dbname = "springboard"
user = "avnadmin"
password = "AVNS_vnii_2iScL7pq6cLyzA"
port = "10548"

# Database connection function
def create_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        return conn
    except Exception as e:
        st.error(f"Error: {e}")
        return None

# Predefined credentials (you can replace this with a database later)
USER_CREDENTIALS = {
    "admin": "test123",
}

# Session state to track login status
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
    st.session_state["username"] = ""

# Login function
def login(username, password):
    if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
        st.session_state["logged_in"] = True
        st.session_state["username"] = username
        st.success(f"Welcome, {username}!")
        st.rerun()  # Immediately rerun the app to reflect the login
    else:
        st.error("Invalid username or password.")

# Logout function
def logout():
    st.session_state["logged_in"] = False
    st.session_state["username"] = ""
    st.rerun()  # Rerun the app to redirect to the login page

# Login page
def show_login_page():
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        login(username, password)

# Load data from the practitioner table
def load_practitioners():
    conn = create_connection()
    if conn:
        try:
            query = "SELECT practitioner_id, practitioner_name, clinic_location, manager_name FROM planning1.practitioner;"
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()
    return pd.DataFrame()

# Streamlit sidebar menu
def streamlit_menu():
    # 1. as sidebar menu
    with st.sidebar:
        selected = option_menu(
            menu_title="Admin Menu",  # required
            options=["Set", "View", "Edit", "Delete"],  # required
            icons=["plus-circle", "book", "pencil-square","trash"],  # optional
            menu_icon="cast",  # optional
            default_index=0,  # optional
        )
    return selected

logo_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTh4_4hTcO4lG08nnLtZYfR6-7lZ0JJi1sBverH4bKkuVarf6tA7j6qHyWGjieIrT7KBBI&usqp=CAU"

# Display the logo in the sidebar
st.sidebar.image(logo_url, use_column_width=True)

# Insert target updates for specified days
def insert_target_updates(practitioner_id, practitioner_name, start_date, end_date, target_hours):
    conn = create_connection()
    records_updated = 0
    conflicts_found = False
    holidays_skipped = []

    if conn:
        try:
            with conn.cursor() as cursor:
                # Fetch all holiday dates
                cursor.execute("SELECT holiday_date FROM planning1.statutory_holidays;")
                holidays = {row[0] for row in cursor.fetchall()}

                date = start_date
                while date <= end_date:
                    weekday = date.strftime('%A')
                    if weekday in target_hours:
                        if date in holidays:
                            holidays_skipped.append(date)
                        else:
                            practitioner_id_int = int(practitioner_id)
                            target_hour = float(target_hours[weekday])
                            target_date = date

                            # Check if a record already exists
                            check_query = """
                            SELECT COUNT(*) FROM planning1.target_update
                            WHERE practitioner_id = %s AND target_date = %s;
                            """
                            cursor.execute(check_query, (practitioner_id_int, target_date))
                            record_exists = cursor.fetchone()[0]

                            if record_exists > 0:
                                conflicts_found = True
                            else:
                                # Insert new target
                                insert_query = """
                                INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                                VALUES (%s, %s, %s, %s, %s);
                                """
                                cursor.execute(insert_query, (practitioner_id_int, practitioner_name, target_date, target_hour, datetime.now()))
                                records_updated += 1
                    date += timedelta(days=1)
                conn.commit()

            # Display appropriate messages based on the results
            if holidays_skipped:
                skipped_dates = ', '.join([date.strftime("%Y-%m-%d") for date in holidays_skipped])
                st.info(f"The following dates were skipped due to statutory holidays: {skipped_dates}")
            if conflicts_found:
                st.error("Some dates already have existing targets. Please use the Edit tab to modify existing records.")
            elif records_updated > 0:
                st.success(f"Target hours set successfully for {records_updated} days!")
            else:
                st.warning("No new targets were set. Please use the Edit tab to modify existing targets.")
        except Exception as e:
            st.error(f"Failed to set target hours: {e}")
        finally:
            conn.close()



# Set target for batch 
def insert_target_updates_batch(practitioners_list, start_date, end_date, target_hours):
    conn = create_connection()
    records_updated = 0
    conflicts_found = False
    holidays_skipped = []

    if conn:
        try:
            with conn.cursor() as cursor:
                # Fetch all holiday dates
                cursor.execute("SELECT holiday_date FROM planning1.statutory_holidays;")
                holidays = {row[0] for row in cursor.fetchall()}

                for practitioner in practitioners_list:
                    practitioner_id = int(practitioner['practitioner_id'])
                    practitioner_name = practitioner['practitioner_name']
                    date = start_date
                    while date <= end_date:
                        weekday = date.strftime('%A')
                        if weekday in target_hours:
                            if date in holidays:
                                holidays_skipped.append(date)
                            else:
                                target_hour = float(target_hours[weekday])

                                # Check if a record already exists
                                check_query = """
                                SELECT COUNT(*) FROM planning1.target_update
                                WHERE practitioner_id = %s AND target_date = %s;
                                """
                                cursor.execute(check_query, (practitioner_id, date))
                                record_exists = cursor.fetchone()[0]

                                if record_exists > 0:
                                    conflicts_found = True
                                else:
                                    # Insert new target
                                    insert_query = """
                                    INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                                    VALUES (%s, %s, %s, %s, %s);
                                    """
                                    cursor.execute(insert_query, (practitioner_id, practitioner_name, date, target_hour, datetime.now()))
                                    records_updated += 1
                        date += timedelta(days=1)
                conn.commit()

            # Display appropriate messages based on the results
            if holidays_skipped:
                skipped_dates = ', '.join([date.strftime("%Y-%m-%d") for date in holidays_skipped])
                st.info(f"The following dates were skipped due to statutory holidays: {skipped_dates}")
            if conflicts_found:
                st.error("Some dates already have existing targets. Please use the Edit tab to modify existing records.")
            elif records_updated > 0:
                st.success(f"Target hours set successfully for {records_updated} days!")
            else:
                st.warning("No new targets were set. Please use the Edit tab to modify existing targets.")
        except Exception as e:
            st.error(f"Failed to set target hours: {e}")
        finally:
            conn.close()



# Load target updates with practitioner name for a specific practitioner and date range
def load_target_updates(practitioner_id, start_date, end_date):
    conn = create_connection()
    if conn:
        try:
            practitioner_id = int(practitioner_id)  # Ensure practitioner_id is a Python int
            query = """
            SELECT p.practitioner_id, p.practitioner_name, t.target_date, t.target_hour 
            FROM planning1.target_update AS t
            JOIN planning1.practitioner AS p ON t.practitioner_id = p.practitioner_id
            WHERE t.practitioner_id = %s AND t.target_date BETWEEN %s AND %s
            ORDER BY t.target_date;
            """
            df = pd.read_sql(query, conn, params=(practitioner_id, start_date, end_date))
            return df
        finally:
            conn.close()
    return pd.DataFrame()

# Helper function for display target updates
def load_practitioner_name(practitioner_id):
    conn = create_connection()
    if conn:
        try:
            query = "SELECT practitioner_name FROM planning.practitioner WHERE practitioner_id = %s;"
            with conn.cursor() as cursor:
                cursor.execute(query, (practitioner_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
        except Exception as e:
            st.error(f"Failed to load practitioner name: {e}")
        finally:
            conn.close()
    return "Unknown"

# Define the function to display target updates
def display_target_updates(selected_practitioners, start_date, end_date):
    # Ensure `selected_practitioners` is a list, even for a single practitioner
    if isinstance(selected_practitioners, int):
        practitioner_id = selected_practitioners
        practitioner_name = load_practitioner_name(practitioner_id)
        selected_practitioners = [{'practitioner_id': practitioner_id, 'practitioner_name': practitioner_name}]
    
    # Prepare a DataFrame to collect target updates for the entire group
    all_target_updates = pd.DataFrame()

    # Loop through each practitioner in the selected group
    for practitioner in selected_practitioners:
        practitioner_id = practitioner['practitioner_id']
        practitioner_name = practitioner['practitioner_name']
        
        # Load target updates for the practitioner
        target_updates_df = load_target_updates(practitioner_id, start_date, end_date)
        
        if not target_updates_df.empty:
            # Add the practitioner's name and ID to the DataFrame for display
            target_updates_df['practitioner_name'] = practitioner_name
            target_updates_df['practitioner_id'] = practitioner_id
            all_target_updates = pd.concat([all_target_updates, target_updates_df], ignore_index=True)

    # Display a single consolidated table if there are records
    if not all_target_updates.empty:
        # Sort the table by practitioner_name and target_date for better readability
        all_target_updates.sort_values(by=['practitioner_name', 'target_date'], inplace=True)
        st.write(f"Consolidated target hours for the selected period ({start_date} to {end_date}):")
        st.dataframe(all_target_updates)
    else:
        st.warning("No target data available for the selected period.")

# Cloning function
import pandas as pd

def clone_target_updates_with_preview(practitioners_list, start_date, end_date):
    conn = create_connection()
    records_cloned = 0
    conflicts_found = False
    holidays_skipped = []
    preview_df = pd.DataFrame()

    if conn:
        try:
            with conn.cursor() as cursor:
                # Fetch all holiday dates
                cursor.execute("SELECT holiday_date FROM planning1.statutory_holidays;")
                holidays = {row[0] for row in cursor.fetchall()}

                # Identify the latest week with target data
                latest_week_query = """
                SELECT target_date
                FROM planning1.target_update
                ORDER BY target_date DESC
                LIMIT 1;
                """
                cursor.execute(latest_week_query)
                latest_target_date = cursor.fetchone()

                if not latest_target_date:
                    st.warning("No past target data available to clone.")
                    return

                latest_target_date = latest_target_date[0]
                latest_start_date = latest_target_date - timedelta(days=latest_target_date.weekday())
                latest_end_date = latest_start_date + timedelta(days=6)  # Full week (Mon-Sun)

                # Fetch target data from the latest available week (Mon-Sun)
                fetch_query = """
                SELECT practitioner_id, practitioner_name, target_date, target_hour
                FROM planning1.target_update
                WHERE target_date BETWEEN %s AND %s;
                """
                cursor.execute(fetch_query, (latest_start_date, latest_end_date))
                past_targets = cursor.fetchall()

                if not past_targets:
                    st.warning("No targets found in the latest week to clone.")
                    return

                # Organize past target data into a preview table
                past_targets_dict = {}
                for row in past_targets:
                    practitioner_id, practitioner_name, target_date, target_hour = row
                    weekday = target_date.strftime('%A')
                    if practitioner_name not in past_targets_dict:
                        past_targets_dict[practitioner_name] = {}
                    past_targets_dict[practitioner_name][weekday] = target_hour

                # Create a DataFrame for preview
                days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                preview_data = []

                for practitioner in practitioners_list:
                    practitioner_name = practitioner['practitioner_name']
                    row = [past_targets_dict.get(practitioner_name, {}).get(day, "") for day in days_of_week]
                    preview_data.append([practitioner_name] + row)

                # Build the preview DataFrame
                preview_df = pd.DataFrame(preview_data, columns=["Practitioner"] + days_of_week)

                # Display the preview DataFrame
                st.subheader("Preview of Target Data to be Cloned")
                st.table(preview_df)

                # Confirm cloning action
                if st.button("Submit Clone Targets"):
                    date = start_date
                    while date <= end_date:
                        weekday = date.strftime('%A')
                        if weekday in days_of_week:
                            for practitioner in practitioners_list:
                                practitioner_id = int(practitioner['practitioner_id'])
                                practitioner_name = practitioner['practitioner_name']

                                if date in holidays:
                                    holidays_skipped.append(date)
                                    continue

                                # Check if a record already exists for this date
                                check_query = """
                                SELECT COUNT(*) FROM planning1.target_update
                                WHERE practitioner_id = %s AND target_date = %s;
                                """
                                cursor.execute(check_query, (practitioner_id, date))
                                record_exists = cursor.fetchone()[0]

                                if record_exists > 0:
                                    conflicts_found = True
                                    continue

                                # Get target hour from the latest week data
                                target_hour = past_targets_dict.get(practitioner_name, {}).get(weekday, None)

                                if target_hour is not None:
                                    # Insert cloned target
                                    insert_query = """
                                    INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                                    VALUES (%s, %s, %s, %s, %s);
                                    """
                                    cursor.execute(insert_query, (practitioner_id, practitioner_name, date, target_hour, datetime.now()))
                                    records_cloned += 1
                        date += timedelta(days=1)
                    conn.commit()

                # Display messages based on the results
                if holidays_skipped:
                    skipped_dates = ', '.join([date.strftime("%Y-%m-%d") for date in holidays_skipped])
                    st.info(f"The following dates were skipped due to statutory holidays: {skipped_dates}")
                if conflicts_found:
                    st.error("Some dates already have existing targets. Please use the Edit tab to modify existing records.")
                elif records_cloned > 0:
                    st.success(f"Cloned target hours successfully for {records_cloned} days!")
                else:
                    st.warning("No new targets were cloned. Please check the selected period or existing targets.")
        except Exception as e:
            st.error(f"Failed to clone target hours: {e}")
        finally:
            conn.close()



# Update target_hour in the database
def update_target_hours(updates):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Prepare the SQL query for batch execution
                query = """
                UPDATE planning1.target_update
                SET target_hour = %s, updated_at = %s
                WHERE practitioner_id = %s AND target_date = %s;
                """

                # Convert updates to a list of tuples for batch execution
                update_values = [
                    (float(update['target_hour']), datetime.now(), int(update['practitioner_id']), update['target_date'])
                    for update in updates
                ]

                # Execute the batch update
                cursor.executemany(query, update_values)
                conn.commit()
                # st.success("Target hours updated successfully!")
        except psycopg2.errors.UniqueViolation:
            st.error("Duplicate entry detected. Please ensure there are no conflicting records.")
        except Exception as e:
            st.error(f"Failed to update target hours: {e}")
        finally:
            conn.close()

# Plot target hours by matplotlib in batch
def plot_target_hours_matplotlib(consolidated_df):
    # Ensure the target_date column is in datetime format
    consolidated_df['target_date'] = pd.to_datetime(consolidated_df['target_date'])

    # Sort the DataFrame by practitioner_name and target_date
    consolidated_df.sort_values(by=['practitioner_name', 'target_date'], inplace=True)

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))

    # Iterate over unique practitioner names and plot each separately
    for practitioner_name in consolidated_df['practitioner_name'].unique():
        # Filter data for the current practitioner
        practitioner_data = consolidated_df[consolidated_df['practitioner_name'] == practitioner_name]

        # Plot the data with a unique line for each practitioner
        ax.plot(
            practitioner_data['target_date'],
            practitioner_data['target_hour'],
            marker='o',
            linestyle='-',
            label=practitioner_name
        )

    # Set title and labels
    ax.set_title("Target Hours Over Time for Selected Practitioners")
    ax.set_xlabel("Date")
    ax.set_ylabel("Target Hour")

    # Format x-axis dates
    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)

    # Display grid for better readability
    ax.grid(True, linestyle='--', alpha=0.6)

    # Add a legend to distinguish lines for different practitioners
    ax.legend(title="Practitioner")

    # Show the plot
    st.pyplot(fig)



# Delete target updates from the database
def delete_target_hours(deletion_records):
    """
    Delete target hours for individual records based on practitioner_id and target_date.
    """
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = "DELETE FROM planning1.target_update WHERE practitioner_id = %s AND target_date = %s;"
                
                # Convert records to a list of tuples for batch execution
                delete_values = [
                    (int(record['practitioner_id']), record['target_date'])
                    for record in deletion_records
                ]

                # Execute batch deletion using executemany()
                cursor.executemany(query, delete_values)
                conn.commit()
                st.success("Selected target hours deleted successfully!")
        except psycopg2.Error as e:
            st.error(f"Failed to delete records: {e}")
        finally:
            conn.close()

# Delete target hours in batches selected from Start - End date
def delete_target_hours_batch(selected_practitioners, start_date, end_date):
    """
    Batch delete target hours for a group of practitioners based on date range.
    """
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = """
                DELETE FROM planning1.target_update
                WHERE practitioner_id = %s AND target_date BETWEEN %s AND %s;
                """

                # Iterate through each practitioner in the selected group
                for practitioner in selected_practitioners:
                    practitioner_id = int(practitioner['practitioner_id'])

                    # Execute the delete query for the current practitioner
                    cursor.execute(query, (practitioner_id, start_date, end_date))
                    conn.commit()  # Commit after each deletion to ensure it is applied

            st.success("Batch deletion completed successfully for the entire team!")
        except Exception as e:
            st.error(f"Failed to delete target hours for the team: {e}")
        finally:
            conn.close()

# Streamlit App
def main():
    st.image("https://springboardclinic.com/wp-content/uploads/2022/05/SBC_Logo_vector.svg")
    st.title("Target Management Application")
    if st.button("Logout"):
        logout()

    st.sidebar.title("🌟 Target Management")
    # Display Options for CRUD Operations
    option = streamlit_menu()

    # Set Target Operation
    if option == "Set":
        st.header("Set Target")
        st.subheader("Select Location, Manager, or Practitioner")

        # Load practitioners data
        practitioners_df = load_practitioners()

        if not practitioners_df.empty:
            # Multiselect for Location
            locations = st.multiselect(
                "Select Location(s)",
                practitioners_df['clinic_location'].unique(),
                default=practitioners_df['clinic_location'].unique()
            )

            # Filter practitioners based on selected locations
            filtered_df = practitioners_df[practitioners_df['clinic_location'].isin(locations)]

            # Multiselect for Manager (filtered based on selected locations)
            managers = st.multiselect(
                "Select Manager(s)",
                filtered_df['manager_name'].unique(),
                default=filtered_df['manager_name'].unique()
            )

            # Further filter practitioners based on selected managers
            filtered_df = filtered_df[filtered_df['manager_name'].isin(managers)]

            # Multiselect for Practitioner (filtered based on selected locations and managers)
            practitioners = st.multiselect(
                "Select Practitioner(s)",
                filtered_df['practitioner_name'].unique(),
                default=filtered_df['practitioner_name'].unique()
            )

            # Final list of selected practitioners
            selected_practitioners = filtered_df[filtered_df['practitioner_name'].isin(practitioners)].to_dict('records')

            # Display selection summary
            if selected_practitioners:
                st.success(f"{len(selected_practitioners)} practitioner(s) selected.")
            else:
                st.warning("No practitioners selected. Please adjust your filters.")

            # Select date range
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")

            # Display current target
            st.subheader('Current Target Table')
            st.markdown(
        """
        <style>
        /* Style the button */
        .refresh-button {
            background-color: white;
            color: orange;
            padding: 10px 24px;
            border: 2px solid orange;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        /* Hover effect */
        .refresh-button:hover {
            background-color: orange;
            color: white;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
            if st.button("Refresh", key="refresh_button"):
                # Clear cached data for the display_target_updates function to fetch the latest data
                st.cache_data.clear()
                # Display the consolidated table with the latest data (cleared cache)
                display_target_updates(selected_practitioners, start_date, end_date)
            else:
                # Display the consolidated table without clearing cache (initial or non-refresh load)
                display_target_updates(selected_practitioners, start_date, end_date)

            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                # Set target hours for weekdays
                st.subheader("Set Target Hours")
                days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                target_hours = {}

                for day in days_of_week:
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        day_selected = st.checkbox(day)
                    if day_selected:
                        with col2:
                            target_hours[day] = float(st.number_input(f"Target Hours for {day}", min_value=0.0, key=day))

                # Submit button
                if st.button("Submit"):
                    if not target_hours:
                        st.warning("Please select at least one day and specify target hours.")
                    elif not selected_practitioners:
                        st.warning("No practitioners found for the selected criteria.")
                    else:
                        # Batch update for all selected practitioners
                        insert_target_updates_batch(selected_practitioners, start_date, end_date, target_hours)
                        # st.success("Target hours updated successfully for the selected group!")
        else:
            st.warning("No practitioners found in the database.")

        
        
    # Placeholder for other operations
    elif option =="View":
        st.header("View Target")
        st.subheader("View Practitioner, Manager or Location")

        # Load practitioners data
        practitioners_df = load_practitioners()

        if not practitioners_df.empty:
            # Multiselect for Location
            locations = st.multiselect(
                "Select Location(s)",
                practitioners_df['clinic_location'].unique(),
                default=practitioners_df['clinic_location'].unique()
            )

            # Filter practitioners based on selected locations
            filtered_df = practitioners_df[practitioners_df['clinic_location'].isin(locations)]

            # Multiselect for Manager (filtered based on selected locations)
            managers = st.multiselect(
                "Select Manager(s)",
                filtered_df['manager_name'].unique(),
                default=filtered_df['manager_name'].unique()
            )

            # Further filter practitioners based on selected managers
            filtered_df = filtered_df[filtered_df['manager_name'].isin(managers)]

            # Multiselect for Practitioner (filtered based on selected locations and managers)
            practitioners = st.multiselect(
                "Select Practitioner(s)",
                filtered_df['practitioner_name'].unique(),
                default=filtered_df['practitioner_name'].unique()
            )

            # Final list of selected practitioners
            selected_practitioners = filtered_df[filtered_df['practitioner_name'].isin(practitioners)].to_dict('records')

            # Display selection summary
            if selected_practitioners:
                st.success(f"{len(selected_practitioners)} practitioner(s) selected.")
            else:
                st.warning("No practitioners selected. Please adjust your filters.")

            # Select date range
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")

            
            # Display current target
            st.subheader('Current Target Table')
            st.markdown(
        """
        <style>
        /* Style the button */
        .refresh-button {
            background-color: white;
            color: orange;
            padding: 10px 24px;
            border: 2px solid orange;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        /* Hover effect */
        .refresh-button:hover {
            background-color: orange;
            color: white;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
            # Refresh button
            if st.button("Refresh", key="refresh_button"):
                # Clear cached data for the display_target_updates function to fetch the latest data
                st.cache_data.clear()
                # Display the consolidated table with the latest data (cleared cache)
                display_target_updates(selected_practitioners, start_date, end_date)
            else:
                # Display the consolidated table without clearing cache (initial or non-refresh load)
                display_target_updates(selected_practitioners, start_date, end_date)


            # View History
            st.subheader('History Target Chart')
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                # Consolidate data for all selected practitioners
                consolidated_df = pd.DataFrame()
                
                for practitioner in selected_practitioners:
                    practitioner_id = practitioner['practitioner_id']
                    practitioner_name = practitioner['practitioner_name']
                    target_updates_df = load_target_updates(practitioner_id, start_date, end_date)

                    if not target_updates_df.empty:
                        # Add practitioner's details to the DataFrame
                        target_updates_df['practitioner_name'] = practitioner_name
                        consolidated_df = pd.concat([consolidated_df, target_updates_df], ignore_index=True)

                # Plot the target hours if data is available
                if not consolidated_df.empty:
                    plot_target_hours_matplotlib(consolidated_df)
                else:
                    st.warning("No target data available for the selected period.")

        else:
            st.warning("No practitioners found in the database.")
    
    
    elif option == "Edit":
        st.header("Edit Target")
        st.subheader("Edit Practitioner, Manager or Location")

        # Load practitioners data
        practitioners_df = load_practitioners()

        if not practitioners_df.empty:
            # Multiselect for Location
            locations = st.multiselect(
                "Select Location(s)",
                practitioners_df['clinic_location'].unique(),
                default=practitioners_df['clinic_location'].unique()
            )

            # Filter practitioners based on selected locations
            filtered_df = practitioners_df[practitioners_df['clinic_location'].isin(locations)]

            # Multiselect for Manager (filtered based on selected locations)
            managers = st.multiselect(
                "Select Manager(s)",
                filtered_df['manager_name'].unique(),
                default=filtered_df['manager_name'].unique()
            )

            # Further filter practitioners based on selected managers
            filtered_df = filtered_df[filtered_df['manager_name'].isin(managers)]

            # Multiselect for Practitioner (filtered based on selected locations and managers)
            practitioners = st.multiselect(
                "Select Practitioner(s)",
                filtered_df['practitioner_name'].unique(),
                default=filtered_df['practitioner_name'].unique()
            )

            # Final list of selected practitioners
            selected_practitioners = filtered_df[filtered_df['practitioner_name'].isin(practitioners)].to_dict('records')

            # Display selection summary
            if selected_practitioners:
                st.success(f"{len(selected_practitioners)} practitioner(s) selected.")
            else:
                st.warning("No practitioners selected. Please adjust your filters.")

            # Select date range
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")

            
            # Display current target
            st.subheader('Current Target Table')
            st.markdown(
        """
        <style>
        /* Style the button */
        .refresh-button {
            background-color: white;
            color: orange;
            padding: 10px 24px;
            border: 2px solid orange;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        /* Hover effect */
        .refresh-button:hover {
            background-color: orange;
            color: white;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
            # Refresh button
            if st.button("Refresh", key="refresh_button"):
                # Clear cached data for the display_target_updates function to fetch the latest data
                st.cache_data.clear()
                # Display the consolidated table with the latest data (cleared cache)
                display_target_updates(selected_practitioners, start_date, end_date)
            else:
                # Display the consolidated table without clearing cache (initial or non-refresh load)
                display_target_updates(selected_practitioners, start_date, end_date)
            
            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                # Load target updates for the selected group
                consolidated_df = pd.DataFrame()

                for practitioner in selected_practitioners:
                    practitioner_id = practitioner['practitioner_id']
                    practitioner_name = practitioner['practitioner_name']
                    target_updates_df = load_target_updates(practitioner_id, start_date, end_date)

                    if not target_updates_df.empty:
                        target_updates_df['practitioner_name'] = practitioner_name
                        consolidated_df = pd.concat([consolidated_df, target_updates_df], ignore_index=True)

                # Check if there is data to edit
                if not consolidated_df.empty:
                    st.subheader("Edit Individual Dates")
                    with st.expander("Click to edit target hours"):
                        st.write("Edit target hours below and click Submit Changes to save individual edits.")

                        # Editable Data Table with Individual Changes Detection
                        edited_df = consolidated_df.copy()
                        updates = []

                        # Collect individual target hour changes
                        for i, row in edited_df.iterrows():
                            new_target_hour = st.number_input(
                                f"Target Hour for {row['target_date']} ({row['practitioner_name']})",
                                min_value=0.0,
                                value=row['target_hour'],
                                key=f"edit_target_hour_{i}"
                            )
                            # Add change to updates list if target_hour is modified
                            if new_target_hour != row['target_hour']:
                                updates.append({
                                    'practitioner_id': row['practitioner_id'],
                                    'target_date': row['target_date'],
                                    'target_hour': new_target_hour
                                })

                        # Submit Changes Button to save individual updates
                        if st.button("Submit Changes"):
                            if updates:
                                update_target_hours(updates)
                                st.success("Individual target hours updated successfully!")
                            else:
                                st.info("No individual changes made.")

                    # Batch Update Section
                    st.subheader("Apply Same Target Hour to All Dates")
                    batch_target_hour = st.number_input("Enter Target Hour for All Dates", min_value=0.0, key="batch_target_hour")
                    if st.button("Apply All"):
                        # Create batch updates for all dates in the selected range
                        batch_updates = [{
                            'practitioner_id': row['practitioner_id'],
                            'target_date': row['target_date'],
                            'target_hour': batch_target_hour
                        } for _, row in edited_df.iterrows()]

                        # Apply batch updates directly to the database
                        update_target_hours(batch_updates)
                        st.success("All dates updated successfully to the specified target hour!")

                else:
                    st.warning("No target data available for the selected period.")
        else:
            st.warning("No practitioners found in the database.")



    elif option == "Delete":
        st.header("Delete Target")
        st.subheader("Delete Practitioner, Manager or Location")

        # Load practitioners data
        practitioners_df = load_practitioners()

        if not practitioners_df.empty:
            # Multiselect for Location
            locations = st.multiselect(
                "Select Location(s)",
                practitioners_df['clinic_location'].unique(),
                default=practitioners_df['clinic_location'].unique()
            )

            # Filter practitioners based on selected locations
            filtered_df = practitioners_df[practitioners_df['clinic_location'].isin(locations)]

            # Multiselect for Manager (filtered based on selected locations)
            managers = st.multiselect(
                "Select Manager(s)",
                filtered_df['manager_name'].unique(),
                default=filtered_df['manager_name'].unique()
            )

            # Further filter practitioners based on selected managers
            filtered_df = filtered_df[filtered_df['manager_name'].isin(managers)]

            # Multiselect for Practitioner (filtered based on selected locations and managers)
            practitioners = st.multiselect(
                "Select Practitioner(s)",
                filtered_df['practitioner_name'].unique(),
                default=filtered_df['practitioner_name'].unique()
            )

            # Final list of selected practitioners
            selected_practitioners = filtered_df[filtered_df['practitioner_name'].isin(practitioners)].to_dict('records')

            # Display selection summary
            if selected_practitioners:
                st.success(f"{len(selected_practitioners)} practitioner(s) selected.")
            else:
                st.warning("No practitioners selected. Please adjust your filters.")

            # Select date range
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")

            
            # Display current target
            st.subheader('Current Target Table')
            st.markdown(
        """
        <style>
        /* Style the button */
        .refresh-button {
            background-color: white;
            color: orange;
            padding: 10px 24px;
            border: 2px solid orange;
            cursor: pointer;
            font-size: 16px;
            border-radius: 5px;
        }

        /* Hover effect */
        .refresh-button:hover {
            background-color: orange;
            color: white;
        }
        </style>
        """,
        unsafe_allow_html=True
    )
            # Refresh button
            if st.button("Refresh", key="refresh_button"):
                # Clear cached data for the display_target_updates function to fetch the latest data
                st.cache_data.clear()
                # Display the consolidated table with the latest data (cleared cache)
                display_target_updates(selected_practitioners, start_date, end_date)
            else:
                # Display the consolidated table without clearing cache (initial or non-refresh load)
                display_target_updates(selected_practitioners, start_date, end_date)
            
            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                # Consolidate target updates for the selected group
                consolidated_df = pd.DataFrame()

                for practitioner in selected_practitioners:
                    practitioner_id = practitioner['practitioner_id']
                    practitioner_name = practitioner['practitioner_name']
                    target_updates_df = load_target_updates(practitioner_id, start_date, end_date)

                    if not target_updates_df.empty:
                        target_updates_df['practitioner_name'] = practitioner_name
                        consolidated_df = pd.concat([consolidated_df, target_updates_df], ignore_index=True)

                # Individual Delete Section
                if not consolidated_df.empty:
                    st.subheader("Delete Individual Dates")
                    with st.expander("Click to select dates for deletion"):
                        st.write("Select rows to delete and click 'Delete Selected Rows'.")

                        # Add a checkbox for deletion selection
                        deletion_records = []
                        for i, row in consolidated_df.iterrows():
                            delete_row = st.checkbox(f"Delete target hour for {row['target_date']} ({row['practitioner_name']}: {row['target_hour']} hours)", key=f"delete_{i}")
                            if delete_row:
                                deletion_records.append({
                                    'practitioner_id': row['practitioner_id'],
                                    'target_date': row['target_date']
                                })

                        # Delete button for individual rows
                        if st.button("Delete Selected Rows"):
                            if deletion_records:
                                delete_target_hours(deletion_records)
                            else:
                                st.info("No rows selected for deletion.")

                    # Batch Delete Section
                    st.subheader("Batch Delete All Dates in Range")
                    if st.button("Delete All Target Hours in Selected Range"):
                        if selected_practitioners:
                            delete_target_hours_batch(selected_practitioners, start_date, end_date)
                        else:
                            st.warning("No practitioners found for the selected criteria.")

                else:
                    st.warning("No target data available for the selected period.")
        else:
            st.warning("No practitioners found in the database.")
# App flow
if st.session_state["logged_in"]:
    main()  # Call your main app here
else:
    show_login_page()

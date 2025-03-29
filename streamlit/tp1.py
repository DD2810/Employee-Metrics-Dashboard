import streamlit as st
import pandas as pd
from datetime import timedelta, datetime, date
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

# Load data from the practitioner table
def load_practitioners():
    conn = create_connection()
    if conn:
        try:
            query = "SELECT practitioner_id, practitioner_name FROM planning1.practitioner;"
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()
    return pd.DataFrame()

# Add "SET ALL" to the sidebar menu function
def streamlit_menu():
    with st.sidebar:
        selected = option_menu(
            menu_title="Admin Menu",  # required
            options=["Set", "Set All", "Clone", "View", "Edit", "Delete", "Summary"],  # added "Set All"
            icons=["plus-circle", "circle", "book", "pencil-square", "trash"],  # optional
            menu_icon="cast",  # optional
            default_index=0,  # optional
        )
    return selected


# Insert target updates for specified days
def insert_target_updates(practitioner_id, practitioner_name, start_date, end_date, target_hours):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                date = start_date
                while date <= end_date:
                    weekday = date.strftime('%A')
                    if weekday in target_hours:
                        # Ensure values are standard Python types
                        practitioner_id_int = int(practitioner_id)  # Ensure practitioner_id is an int
                        target_hour_float = float(target_hours[weekday])  # Ensure target_hour is a float
                        target_date = date  # Ensure date is datetime.date or datetime.datetime

                        query = """
                        INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(query, (
                        practitioner_id_int, practitioner_name, target_date, target_hour_float, datetime.now()))
                    date += timedelta(days=1)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to update targets: {e}")
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


# Display target updates in a table format based on practitioner and date range
def display_target_updates(practitioner_id, start_date, end_date):
    target_updates_df = load_target_updates(practitioner_id, start_date, end_date)
    if not target_updates_df.empty:
        st.write(
            f"Target hours for {target_updates_df['practitioner_name'].iloc[0]} (ID: {practitioner_id}) over the selected period:")
        st.dataframe(target_updates_df)
    else:
        st.warning("No target data available for the selected period.")


# Function to load unique manager IDs for the dropdown
def load_manager_names():
    conn = create_connection()
    if conn:
        try:
            query = "SELECT DISTINCT manager_name FROM planning1.practitioner WHERE manager_name IS NOT NULL ORDER BY manager_name;"
            df = pd.read_sql(query, conn)
            return df['manager_name'].tolist()
        finally:
            conn.close()
    return []

# Function to load and aggregate target data for summary filtered by manager_name
def load_summary_data(aggregation='weekly', manager_name=None):
    conn = create_connection()
    if conn:
        try:
            query = """
            SELECT p.manager_name, t.practitioner_id, t.target_date, t.target_hour
            FROM planning1.target_update AS t
            JOIN planning1.practitioner AS p ON t.practitioner_id = p.practitioner_id
            """
            if manager_name:
                query += " WHERE p.manager_name = %s"
                df = pd.read_sql(query, conn, params=(manager_name,))
            else:
                df = pd.read_sql(query, conn)

            # Convert target_date to datetime if it's not already
            df['target_date'] = pd.to_datetime(df['target_date'])

            # Aggregate based on the chosen period
            if aggregation == 'weekly':
                df['week'] = df['target_date'].dt.to_period('W').apply(lambda r: r.start_time)
                summary = df.groupby('week')['target_hour'].sum().reset_index()
                summary.rename(columns={'week': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            elif aggregation == 'monthly':
                df['month'] = df['target_date'].dt.to_period('M').apply(lambda r: r.start_time)
                summary = df.groupby('month')['target_hour'].sum().reset_index()
                summary.rename(columns={'month': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            elif aggregation == 'quarterly':
                df['quarter'] = df['target_date'].dt.to_period('Q').apply(lambda r: r.start_time)
                summary = df.groupby('quarter')['target_hour'].sum().reset_index()
                summary.rename(columns={'quarter': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            else:
                st.error("Invalid aggregation type selected.")
                return pd.DataFrame()

            return summary
        finally:
            conn.close()
    return pd.DataFrame()


# Update target_hour in the database
def update_target_hours(updates):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                for update in updates:
                    query = """
                    UPDATE planning1.target_update
                    SET target_hour = %s, updated_at = %s
                    WHERE practitioner_id = %s AND target_date = %s;
                    """
                    cursor.execute(query, (
                    update['target_hour'], datetime.now(), update['practitioner_id'], update['target_date']))
                conn.commit()
        except Exception as e:
            st.error(f"Failed to update target hours: {e}")
        finally:
            conn.close()


# Plot target hours by matplotlib
def plot_target_hours_matplotlib(target_updates_df):
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the target hours with markers
    ax.plot(target_updates_df['target_date'], target_updates_df['target_hour'], marker='o', linestyle='-', color='b')

    # Set title and labels
    ax.set_title("Target Hours Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Target Hour")

    # Rotate x-axis dates
    plt.xticks(rotation=90)

    # Display grid for easier visualization
    ax.grid(True, linestyle='--', alpha=0.6)

    # Show the plot
    st.pyplot(fig)


# Delete target updates from the database
def delete_target_hours(deletion_records):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = "DELETE FROM planning1.target_update WHERE practitioner_id = %s AND target_date = %s;"
                for record in deletion_records:
                    cursor.execute(query, (record['practitioner_id'], record['target_date']))
                conn.commit()
        except Exception as e:
            st.error(f"Failed to delete records: {e}")
        finally:
            conn.close()


# Delete target hours in batches selected from Start - End date
def delete_target_hours_batch(practitioner_id, start_date, end_date):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = """
                DELETE FROM planning1.target_update
                WHERE practitioner_id = %s AND target_date BETWEEN %s AND %s;
                """
                cursor.execute(query, (int(practitioner_id), start_date, end_date))
                conn.commit()
        except Exception as e:
            st.error(f"Failed to delete target hours: {e}")
        finally:
            conn.close()


# Function to load practitioners based on location
# Load target updates for all practitioners within a location and date range
# Function to load target updates for practitioners based on location and date range
def load_target_updates(location=None, manager_name=None, start_date=None, end_date=None):
    conn = create_connection()
    if conn:
        try:
            query = """
            SELECT p.practitioner_id, p.practitioner_name, p.clinic_location, p.manager_name, t.target_date, t.target_hour
            FROM planning1.target_update AS t
            JOIN planning1.practitioner AS p ON t.practitioner_id = p.practitioner_id
            """
            params = []

            # Add filters based on the user's selection
            if location and location != "All":
                query += "WHERE p.clinic_location = %s AND t.target_date BETWEEN %s AND %s "
                params.extend([location, start_date, end_date])
            elif manager_name and manager_name != "All":
                query += "WHERE p.manager_name = %s AND t.target_date BETWEEN %s AND %s "
                params.extend([manager_name, start_date, end_date])
            else:
                query += "WHERE t.target_date BETWEEN %s AND %s "
                params.extend([start_date, end_date])

            query += "ORDER BY t.target_date;"

            df = pd.read_sql(query, conn, params=params)
            return df
        finally:
            conn.close()
    return pd.DataFrame()

# Function to set targets for all practitioners within a location
# Function to update target hours for all practitioners within a location
def set_targets_for_all_practitioners(location=None, manager_name=None, start_date=None, end_date=None, target_hour=0.0):
    # Load practitioners based on the user's selection criteria (location or manager)
    practitioners_df = load_target_updates(location=location, manager_name=manager_name, start_date=start_date,
                                           end_date=end_date)

    if practitioners_df.empty:
        st.warning("No practitioners found for the selected criteria and date range.")
        return False  # Indicate that no updates were made

    conn = create_connection()
    updated = False
    if conn:
        try:
            with conn.cursor() as cursor:
                for _, row in practitioners_df.iterrows():
                    practitioner_id = row['practitioner_id']
                    practitioner_name = row['practitioner_name']
                    date = start_date

                    # Loop through each date in the range and update the target hour
                    while date <= end_date:
                        query = """
                        INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (practitioner_id, target_date) 
                        DO UPDATE SET target_hour = EXCLUDED.target_hour, updated_at = EXCLUDED.updated_at;
                        """
                        cursor.execute(
                            query,
                            (practitioner_id, practitioner_name, date, target_hour, datetime.now())
                        )
                        date += timedelta(days=1)

                conn.commit()
                updated = True  # Indicate that updates were made
        except Exception as e:
            st.error(f"Failed to update targets: {e}")
        finally:
            conn.close()

    return updated

# Function to load unique clinic locations from the practitioner table
def load_clinic_locations():
    conn = create_connection()
    if conn:
        try:
            query = "SELECT DISTINCT clinic_location FROM planning1.practitioner WHERE clinic_location IS NOT NULL;"
            df = pd.read_sql(query, conn)
            return df['clinic_location'].tolist()
        finally:
            conn.close()
    return []


# Function to load and aggregate target data for summary
def load_summary_data(aggregation='weekly', manager_name=None):
    conn = create_connection()
    if conn:
        try:
            query = """
            SELECT practitioner_id, target_date, target_hour
            FROM planning1.target_update;
            """
            df = pd.read_sql(query, conn)

            # Convert target_date to datetime if it's not already
            df['target_date'] = pd.to_datetime(df['target_date'])

            # Aggregate based on the chosen period
            if aggregation == 'weekly':
                df['week'] = df['target_date'].dt.to_period('W').apply(lambda r: r.start_time)
                summary = df.groupby('week')['target_hour'].sum().reset_index()
                summary.rename(columns={'week': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            elif aggregation == 'monthly':
                df['month'] = df['target_date'].dt.to_period('M').apply(lambda r: r.start_time)
                summary = df.groupby('month')['target_hour'].sum().reset_index()
                summary.rename(columns={'month': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            elif aggregation == 'quarterly':
                df['quarter'] = df['target_date'].dt.to_period('Q').apply(lambda r: r.start_time)
                summary = df.groupby('quarter')['target_hour'].sum().reset_index()
                summary.rename(columns={'quarter': 'Period', 'target_hour': 'Total Target Hours'}, inplace=True)
            else:
                st.error("Invalid aggregation type selected.")
                return pd.DataFrame()

            return summary
        finally:
            conn.close()
    return pd.DataFrame()

# Function to load target data for cloning from a previous period
def load_clone_data(period_type, manager_name=None, max_date=None):
    conn = create_connection()
    if conn:
        try:
            query = """
            SELECT p.manager_name, t.practitioner_id, t.target_date, t.target_hour
            FROM planning1.target_update AS t
            JOIN planning1.practitioner AS p ON t.practitioner_id = p.practitioner_id
            """
            filters = []
            params = []

            # Add the manager filter if specified
            if manager_name:
                filters.append("p.manager_name = %s")
                params.append(manager_name)

            # Determine the date range for the selected period type
            now = datetime.now() if max_date is None else pd.to_datetime(max_date)
            if period_type == 'Previous Week':
                start_date = (now - timedelta(weeks=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = now - timedelta(days=now.weekday() + 1)
            elif period_type == 'Previous Month':
                first_day_of_current_month = now.replace(day=1)
                start_date = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
                end_date = first_day_of_current_month - timedelta(days=1)
            elif period_type == 'Previous Quarter':
                current_quarter = (now.month - 1) // 3 + 1
                start_month_of_current_quarter = 3 * (current_quarter - 1) + 1
                first_day_of_current_quarter = now.replace(month=start_month_of_current_quarter, day=1)
                start_date = (first_day_of_current_quarter - timedelta(days=1)).replace(day=1)
                end_date = first_day_of_current_quarter - timedelta(days=1)
            elif period_type == 'Previous Year':
                start_date = (now.replace(year=now.year - 1, month=1, day=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = (now.replace(year=now.year - 1, month=12, day=31)).replace(hour=23, minute=59, second=59)
            else:
                st.error("Invalid period type selected.")
                return pd.DataFrame()

            # Ensure that the end_date is before the target_start_date
            if max_date:
                filters.append("t.target_date <= %s")
                params.append(max_date)

            filters.append("t.target_date BETWEEN %s AND %s")
            params.extend([start_date, end_date])

            if filters:
                query += " WHERE " + " AND ".join(filters)

            df = pd.read_sql(query, conn, params=params)
            return df
        finally:
            conn.close()
    return pd.DataFrame()

# Function to clone target data to a new date range
def clone_targets_to_date_range(clone_data, target_start_date, target_end_date):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                for _, row in clone_data.iterrows():
                    practitioner_id = row['practitioner_id']
                    target_hour = row['target_hour']

                    # Loop through the target date range
                    current_date = target_start_date
                    while current_date <= target_end_date:
                        query = """
                        INSERT INTO planning1.target_update (practitioner_id, target_date, target_hour, updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (practitioner_id, target_date)
                        DO UPDATE SET target_hour = EXCLUDED.target_hour, updated_at = EXCLUDED.updated_at;
                        """
                        cursor.execute(query, (practitioner_id, current_date, target_hour, datetime.now()))
                        current_date += timedelta(days=1)

                conn.commit()
                st.success("Targets cloned successfully to the selected date range.")
        except Exception as e:
            st.error(f"Failed to clone targets: {e}")
        finally:
            conn.close()

# Function for checking if the start date is valid (not before today's date)
def validate_start_date(start_date):
    today = date.today()
    if start_date < today:
        st.error("Start Date must not be before today's date.")
        return False
    return True

# Integrate the update and refresh functionality in the main function with session state
def main():
    st.image("https://springboardclinic.com/wp-content/uploads/2022/05/SBC_Logo_vector.svg")
    st.title("Target Management Application")

    st.sidebar.title("🌟 Target Management")
    option = streamlit_menu()

    # Initialize session state for storing intermediate state
    if "show_results" not in st.session_state:
        st.session_state["show_results"] = False
    if "update_success" not in st.session_state:
        st.session_state["update_success"] = False

    if option == "Summary":
        st.header("Company-wide Target Summary")

        # Load manager IDs for the dropdown
        manager_names = ["All"] + load_manager_names()

        # Select aggregation period (weekly, monthly, quarterly)
        aggregation = st.selectbox("Select Aggregation Period", ['Weekly', 'Monthly', 'Quarterly'])

        # Manager ID dropdown selection
        selected_manager_name = st.selectbox("Select Team", manager_names)

        # Convert selected manager ID to None if "All" is selected
        manager_name_filter = None if selected_manager_name == "All" else selected_manager_name

        # Load and display the summary data
        if st.button("Show Summary"):
            summary_data = load_summary_data(aggregation.lower(), manager_name_filter)
            if not summary_data.empty:
                st.subheader(f"Total Target Hours Summary - {aggregation}")
                if manager_name_filter:
                    st.subheader(f"Summary for Manager ID: {manager_name_filter}")
                st.dataframe(summary_data)

                # Plot the summary data
                st.write("Visual Representation:")
                fig, ax = plt.subplots()
                ax.plot(summary_data['Period'], summary_data['Total Target Hours'], marker='o', linestyle='-', linewidth=2)
                ax.set_title(f"Total Target Hours ({aggregation} Basis)")
                ax.set_xlabel('Period')
                ax.set_ylabel('Total Target Hours')
                plt.xticks(rotation=45)
                st.pyplot(fig)
            else:
                st.warning("No data available for the selected aggregation period and manager ID.")

    if option == "Clone":
        st.header("Clone Targets from a Previous Period")

        # Step 1: Select Manager
        st.subheader("Select Manager")
        manager_names = ["All"] + load_manager_names()
        selected_manager_name = st.selectbox("Select Manager ID", manager_names)
        manager_name_filter = None if selected_manager_name == "All" else selected_manager_name

        # Step 2: Select start and end date for the target range to be updated
        st.subheader("Select Target Date Range")
        target_start_date = st.date_input("Select Target Start Date")
        target_end_date = st.date_input("Select Target End Date")

        # Validate start date
        if not validate_start_date(target_start_date):
            return  # Stop execution if the start date is invalid

        if target_start_date > target_end_date:
            st.error("End Date must be after Start Date.")
        else:
            # Step 3: Choose the period to clone from
            st.subheader("Select Period to Clone From")
            period_type = st.selectbox("Clone From",
                                       ['Previous Week', 'Previous Month', 'Previous Quarter', 'Previous Year'])

            # Load the data for cloning
            if st.button("Load Data for Cloning"):
                clone_data = load_clone_data(period_type, manager_name_filter)

                if not clone_data.empty:
                    st.subheader(f"Target Data from {period_type} (before {target_start_date})")
                    st.dataframe(clone_data)

                    # Clone button to copy data to the new date range
                    if st.button("Clone to Selected Date Range"):
                        clone_targets_to_date_range(clone_data, target_start_date, target_end_date)
                else:
                    st.warning("No data available for cloning for the selected period and manager.")

    # Initialize session state for selection criteria if not already set
    if "selection_criteria" not in st.session_state:
        st.session_state["selection_criteria"] = None
    if "show_results" not in st.session_state:
        st.session_state["show_results"] = False

    # Initialize the variables
    location = None
    selected_manager = None

    # "SET ALL" option
    if option == "Set All":
        st.header("Set Target for All Practitioners")

        # Step 1: Show buttons for choosing selection criteria
        if st.session_state["selection_criteria"] is None:
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Select by Manager"):
                    st.session_state["selection_criteria"] = "manager"

            with col2:
                if st.button("Select by Location"):
                    st.session_state["selection_criteria"] = "location"

        # Step 2: Display the dropdown based on the selected criteria
        if st.session_state["selection_criteria"] == "manager":
            # Load unique manager IDs and display the dropdown
            manager_names = ["All"] + load_manager_names()
            selected_manager = st.selectbox("Select Manager ID", manager_names)
            location = None  # No location filter needed when selecting by manager

        elif st.session_state["selection_criteria"] == "location":
            # Load unique locations and display the dropdown
            locations = load_clinic_locations()
            location = st.selectbox("Select Location", ["All"] + locations)
            selected_manager = None  # No manager filter needed when selecting by location

        # Select date range
        start_date = st.date_input("Start Date", key="set_all_start_date")
        end_date = st.date_input("End Date", key="set_all_end_date")

        # Create columns for layout
        col1, col2 = st.columns([1, 1])

        # Show Practitioners button
        with col1:
            if st.button("Show Practitioners and Targets"):
                # Validate start date
                if not validate_start_date(start_date):
                    return  # Stop execution if the start date is invalid

                # Validate date range
                if start_date > end_date:
                    st.error("End Date must be after Start Date.")
                    return  # Stop execution if date range is invalid

                # Check if data is available for the selected date range for practitioners
                practitioners_df = load_target_updates(location=location, manager_name=selected_manager,
                                                       start_date=start_date, end_date=end_date)
                if practitioners_df.empty:
                    st.error("No data available for the selected date range for practitioners.")
                    return  # Stop execution if no data is available

                # Set state to show results if all checks pass
                st.session_state["show_results"] = True

        # Refresh button
        with col2:
            if st.button("Refresh Data"):
                st.session_state["show_results"] = True  # Trigger data refresh

        # Display results if the state is set
        if st.session_state["show_results"]:
            # Load target data for the selected criteria and date range
            target_updates_df = load_target_updates(location=location, manager_name=selected_manager,
                                                    start_date=start_date, end_date=end_date)

            if not target_updates_df.empty:
                # Display target data in a table format
                if st.session_state["selection_criteria"] == "location":
                    st.subheader(
                        f"Target Data for Practitioners in {location if location != 'All' else 'all locations'}")
                elif st.session_state["selection_criteria"] == "manager":
                    st.subheader(f"Target Data for Practitioners Managed by Manager ID: {selected_manager}")

                st.dataframe(target_updates_df)

                # Input for setting target hours after displaying results
                st.write("Now, you can set new target hours for all these practitioners.")
                target_hour = st.number_input("Enter Target Hour for All Practitioners", min_value=0.0,
                                              key="set_all_target_hour")

                # Button to submit the new target hour
                if st.button("Submit Target Hour for All"):
                    if target_hour <= 0:
                        st.warning("Please specify a valid target hour greater than 0.")
                    else:
                        # Call the function to update the database
                        update_success = set_targets_for_all_practitioners(location=location,
                                                                           manager_name=selected_manager,
                                                                           start_date=start_date, end_date=end_date,
                                                                           target_hour=target_hour)
                        if update_success:
                            st.session_state["update_success"] = True  # Set state for update success

        # Check if the update was successful and display the success message
        if st.session_state.get("update_success", False):
            st.success("Target hours updated successfully for the selected practitioners and date range!")

            # Refresh and display updated target data
            updated_target_updates_df = load_target_updates(location=location, manager_name=selected_manager,
                                                            start_date=start_date, end_date=end_date)
            st.dataframe(updated_target_updates_df)

            # Reset state after showing the updated data
            st.session_state["update_success"] = False


# Run the main function
if __name__ == "__main__":
    main()
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
                        cursor.execute(query, (practitioner_id_int, practitioner_name, target_date, target_hour_float, datetime.now()))
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

# Define the function to display target updates
def display_target_updates(practitioner_id, start_date, end_date):
    target_updates_df = load_target_updates(practitioner_id, start_date, end_date)
    if not target_updates_df.empty:
        st.write(f"Target hours for {target_updates_df['practitioner_name'].iloc[0]} (ID: {practitioner_id}) over the selected period:")
        st.dataframe(target_updates_df)
    else:
        st.warning("No target data available for the selected period.")


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
                    cursor.execute(query, (update['target_hour'], datetime.now(), update['practitioner_id'], update['target_date']))
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


def get_week_of_month(date):
    """
    Returns the week (1st, 2nd, 3rd, or 4th) of the given weekday in the month.
    """
    first_day_of_month = date.replace(day=1)
    weekday_of_first_day = first_day_of_month.weekday()
    day_difference = (date.day - 1) // 7
    week_of_month = day_difference + 1  # 1-based index for weeks
    return week_of_month, date.weekday()  # weekday() returns 0 for Monday, 1 for Tuesday, etc.


def get_source_date_range(target_start_date, target_end_date, clone_option):
    """
    Calculates the source date range based on the target start date's week of the month and weekday.
    """
    # Determine the week and weekday for the target_start_date
    week_of_month, target_weekday = get_week_of_month(target_start_date)

    if clone_option == "last_month":
        # Calculate the equivalent date range in the previous month
        source_month = target_start_date.month - 1 or 12
        source_year = target_start_date.year if target_start_date.month > 1 else target_start_date.year - 1
    elif clone_option == "last_year":
        # Calculate the equivalent date range in the previous year
        source_month = target_start_date.month
        source_year = target_start_date.year - 1
    else:
        raise ValueError("Invalid clone option")

    # Find the correct source date that matches the week and weekday of the target date
    first_day_of_source_month = target_start_date.replace(year=source_year, month=source_month, day=1)
    source_start_date = get_nth_weekday_in_month(first_day_of_source_month, target_weekday, week_of_month)
    source_end_date = source_start_date + (target_end_date - target_start_date)

    return source_start_date, source_end_date


def get_nth_weekday_in_month(date, weekday, nth):
    """
    Returns the nth occurrence of a specific weekday in the month of the given date.
    """
    first_day_of_month = date.replace(day=1)
    # Calculate the difference to the first instance of the target weekday in the month
    days_to_first_weekday = (weekday - first_day_of_month.weekday() + 7) % 7
    nth_weekday = first_day_of_month + timedelta(days=days_to_first_weekday + (nth - 1) * 7)
    return nth_weekday if nth_weekday.month == date.month else None  # Ensure it's in the same month

def clone_target_hours(practitioner_id, practitioner_name, target_start_date, target_end_date,
                       clone_option="last_month"):
    # Calculate the source date range based on the selected clone option
    if clone_option == "last_week" and (target_end_date - target_start_date).days == 5:
        # Calculate the same 5-day range in the previous week
        source_start_date = target_start_date - timedelta(weeks=1)
        source_end_date = target_end_date - timedelta(weeks=1)
    else:
        # Calculate the source date range for last month or last year
        source_start_date, source_end_date = get_source_date_range(target_start_date, target_end_date, clone_option)

    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                # Step 1: Check for existing target hours in the target date range
                check_query = """
                SELECT COUNT(*) FROM planning1.target_update
                WHERE practitioner_id = %s AND target_date BETWEEN %s AND %s;
                """
                cursor.execute(check_query, (int(practitioner_id), target_start_date, target_end_date))
                existing_count = cursor.fetchone()[0]

                if existing_count > 0:
                    st.error(
                        f"Target hours already exist for practitioner ID {practitioner_id} ({practitioner_name}) in the selected target date range.")
                    return

                # Step 2: Fetch target hours for the practitioner from the source period
                fetch_query = """
                SELECT target_date, target_hour FROM planning1.target_update
                WHERE practitioner_id = %s AND target_date BETWEEN %s AND %s
                ORDER BY target_date;
                """
                cursor.execute(fetch_query, (int(practitioner_id), source_start_date, source_end_date))
                cloned_data = cursor.fetchall()

                # Check if there is no data in the source period
                if not cloned_data:
                    st.error("No target data available for the selected period. Select a different date range.")
                    return

                # Step 3: Prepare data to insert into the target period
                insert_data = []
                day_difference = (target_start_date - source_start_date).days
                for row in cloned_data:
                    source_date, target_hour = row
                    target_date = source_date + timedelta(days=day_difference)

                    # Convert values to standard Python types for psycopg2
                    insert_data.append((
                        int(practitioner_id),  # Convert practitioner_id to int
                        str(practitioner_name),  # Add practitioner_name
                        target_date,
                        float(target_hour),  # Convert target_hour to float
                        datetime.now()
                    ))

                # Step 4: Insert cloned data into the target period
                insert_query = """
                INSERT INTO planning1.target_update (practitioner_id, practitioner_name, target_date, target_hour, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (practitioner_id, target_date)
                DO UPDATE SET target_hour = EXCLUDED.target_hour, updated_at = EXCLUDED.updated_at;
                """
                cursor.executemany(insert_query, insert_data)
                conn.commit()

                st.success(
                    f"Target hours cloned successfully for practitioner ID {practitioner_id} ({practitioner_name}) from {source_start_date} to {source_end_date}.")
        except Exception as e:
            st.error(f"Failed to clone target hours: {e}")
        finally:
            conn.close()


# Streamlit App
def main():
    st.image("https://springboardclinic.com/wp-content/uploads/2022/05/SBC_Logo_vector.svg")
    st.title("Target Management Application")

    st.sidebar.title("🌟 Target Management")
    # Display Options for CRUD Operations
    option = streamlit_menu()

    # option = st.sidebar.radio("Select an Operation", ["Set", "View", "Edit", "Delete"])

    # Set Target Operation
    if option == "Set":
        st.header("Set Target")
        st.subheader("Select")
        # Load practitioners data
        practitioners_df = load_practitioners()

        # Select practitioner
        if not practitioners_df.empty:
            practitioner_name = st.selectbox("Select Practitioner", practitioners_df['practitioner_name'])
            selected_id = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name]['practitioner_id'].values[0]

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

            # Display the button with custom CSS class
            # if st.markdown('<button class="refresh-button">Refresh</button>', unsafe_allow_html=True):
            if st.button("Refresh", key="refresh_button"):
        # Clear cached data for the display_target_updates function to fetch the latest data
                st.cache_data.clear()
                # Display the table with the latest data
                display_target_updates(selected_id, start_date, end_date)
            else:
                # Display data without clearing cache (initial or non-refresh load)
                display_target_updates(selected_id, start_date, end_date)
            
                # Display only "Set Manually" and "Clone" actions when a specific practitioner is selected
            action = st.radio("Choose Action", ["Set Target", "Clone from Previous Period"])

            if action == "Set Target":
                # Set target on weekdays
                st.subheader("Set Target")
                # Validate date range
                if start_date > end_date:
                    st.error("End Date must be after Start Date.")
                else:
                    st.write("Select target hours for each day you want to update:")

                    # Day selection with target hour input fields
                    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
                    target_hours = {}

                    for day in days_of_week:
                        col1, col2 = st.columns([1, 2])
                        with col1:
                            day_selected = st.checkbox(day)
                        if day_selected:
                            with col2:
                                # Ensure target_hours entries are converted to float
                                target_hours[day] = float(st.number_input(f"Target Hours for {day}", min_value=0.0, key=day))

                    # Submit button
                    if st.button("Submit"):
                        if not target_hours:
                            st.warning("Please select at least one day and specify target hours.")
                        else:
                            insert_target_updates(selected_id, practitioner_name, start_date, end_date, target_hours)
                            st.success("Target hours updated successfully for the selected date range and days!")

            elif action == "Clone from Previous Period":
                st.header("Clone Target Hours from Previous Period")

                # Use the previously selected start_date and end_date as the target date range
                target_start_date = start_date
                target_end_date = end_date

                if (target_end_date - target_start_date).days+1 == 5:
                    clone_option = st.selectbox("Clone From", ["Previous Week", "Same Days of Previous Month", "Same Days of Previous Year"])
                else:
                    clone_option = st.selectbox("Clone From", ["Same Days of Previous Month", "Same Days of Previous Year"])

                # Calculate the source date range based on the selected cloning option
                if clone_option == "Same Days of Previous Month":
                    # Calculate the same date range in the previous month
                    source_start_date = target_start_date - timedelta(days=target_start_date.day)
                    source_start_date = source_start_date.replace(day=target_start_date.day)
                    source_end_date = source_start_date + (target_end_date - target_start_date)
                elif clone_option == "Same Days of Previous Year":
                    # Calculate the same date range in the previous year
                    source_start_date = target_start_date.replace(year=target_start_date.year - 1)
                    source_end_date = target_end_date.replace(year=target_end_date.year - 1)

                # Clone button to initiate the cloning process
                if st.button("Clone Target Hours"):
                    selected_id = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name][
                        'practitioner_id'].values[0]
                    selected_name = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name][
                        'practitioner_name'].values[0]

                    # Execute the cloning function
                    clone_target_hours(selected_id, selected_name, target_start_date, target_end_date)

            else:
                st.warning("No practitioners found in the database.")


    # Placeholder for other operations
    if option =="View":
        st.header("View Target")
        st.subheader("Select")
        # Load practitioners data
        practitioners_df = load_practitioners()

        # Select practitioner to view
        if not practitioners_df.empty:
            practitioner_name = st.selectbox("Select Practitioner", practitioners_df['practitioner_name'], key="view_practitioner")
            selected_id = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name]['practitioner_id'].values[0]

            # Date range selection
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")

            with col2:
                end_date = st.date_input("End Date")

            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
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
                    # Display the table with the latest data
                    display_target_updates(selected_id, start_date, end_date)
                else:
                    # Display data without clearing cache (initial or non-refresh load)
                    display_target_updates(selected_id, start_date, end_date)

                # Plot line chart for target update history
                st.subheader("View Target History")

                # Load data for plotting
                target_updates_df = load_target_updates(selected_id, start_date, end_date)

                if not target_updates_df.empty:
                    plot_target_hours_matplotlib(target_updates_df)
                else:
                    st.warning("No target data available for the selected period.")

        else:
            st.warning("No practitioners found in the database.")
    
    
    if option == "Edit":
        st.header("Edit Target")
        st.subheader("Select")
        # Load practitioners data
        practitioners_df = load_practitioners()

        # Select practitioner to edit
        if not practitioners_df.empty:
            practitioner_name = st.selectbox("Select Practitioner", practitioners_df['practitioner_name'], key="edit_practitioner")
            selected_id = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name]['practitioner_id'].values[0]

            # Date range selection
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", key="edit_start_date")

            with col2:
                end_date = st.date_input("End Date", key="edit_end_date")
            
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
                # Display the table with the latest data
                display_target_updates(selected_id, start_date, end_date)
            else:
                # Display data without clearing cache (initial or non-refresh load)
                display_target_updates(selected_id, start_date, end_date)
            
            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                st.subheader("Edit Individual Date")
                # Load target updates within the selected period
                target_updates_df = load_target_updates(selected_id, start_date, end_date)
                with st.expander("Click to edit target hours"):
                    if not target_updates_df.empty:
                        st.write("Edit target hours below and click Submit Changes to save individual edits.")
                        
                        # Editable Data Table with Individual Changes Detection
                        edited_df = target_updates_df.copy()
                        updates = []

                        # Collect individual target hour changes
                        for i, row in edited_df.iterrows():
                            new_target_hour = st.number_input(
                                f"Target Hour for {row['target_date']}",
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
                    
                
                # Apply the same target hour to all dates in the selected period
                if not target_updates_df.empty:
                    st.subheader("Apply Same Target Hour to All Dates")
                    batch_target_hour = st.number_input("Enter Target Hour for All Dates", min_value=0.0, key="batch_target_hour")
                    if st.button("Apply All"):
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
        st.subheader("Select")
        # Load practitioners data
        practitioners_df = load_practitioners()

        # Select practitioner to delete target hours
        if not practitioners_df.empty:
            practitioner_name = st.selectbox("Select Practitioner", practitioners_df['practitioner_name'], key="delete_practitioner")
            selected_id = practitioners_df[practitioners_df['practitioner_name'] == practitioner_name]['practitioner_id'].values[0]

            # Date range selection
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", key="edit_start_date")

            with col2:
                end_date = st.date_input("End Date", key="edit_end_date")

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
                # Display the table with the latest data
                display_target_updates(selected_id, start_date, end_date)
            else:
                # Display data without clearing cache (initial or non-refresh load)
                display_target_updates(selected_id, start_date, end_date)
            
            # Validate date range
            if start_date > end_date:
                st.error("End Date must be after Start Date.")
            else:
                # Load target updates within the selected period
                st.subheader("Delete Individual Date")
                target_updates_df = load_target_updates(selected_id, start_date, end_date)
            
                with st.expander("Click to edit target hours"):
                    if not target_updates_df.empty:
                        st.write("Select rows to delete and click 'Delete Selected Rows'.")

                        # Add a checkbox for deletion selection
                        deletion_records = []
                        for i, row in target_updates_df.iterrows():
                            delete_row = st.checkbox(f"Delete target hour for {row['target_date']} ({row['target_hour']} hours)", key=f"delete_{i}")
                            if delete_row:
                                deletion_records.append({
                                    'practitioner_id': row['practitioner_id'],
                                    'target_date': row['target_date']
                                })

                        # Delete button
                        if st.button("Delete Selected Rows"):
                            if deletion_records:
                                delete_target_hours(deletion_records)
                                st.success("Selected rows deleted successfully!")
                            else:
                                st.info("No rows selected for deletion.")


                    # Batch Delete Section
                
                if target_updates_df.empty:
                    st.warning("No target data available for the selected period.")
                else:
                    st.subheader("Batch Delete All Dates in Range")
                    if st.button("Delete All Target Hours in Selected Range"):
                        delete_target_hours_batch(selected_id, start_date, end_date)
                        st.success(f"All target hours from {start_date} to {end_date} for {practitioner_name} have been deleted.")
        else:
            st.warning("No practitioners found in the database.")


# Run the main function
if __name__ == "__main__":
    main()
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
with DAG(
    'daily_star_schema_etl',
    default_args=default_args,
    description='ETL pipeline to load data into star schema daily',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Step 1: Load new data into Dim_Location
    load_dim_location = PostgresOperator(
        task_id='load_dim_location',
        postgres_conn_id='springboard_online',
        sql="""
        INSERT INTO model.Dim_Location (location_name)
        SELECT DISTINCT location
        FROM public.input
        WHERE location IS NOT NULL
          AND location NOT IN (SELECT location_name FROM model.Dim_Location);
        """
    )

    # Step 2: Load new data into Dim_Item
    load_dim_item = PostgresOperator(
        task_id='load_dim_item',
        postgres_conn_id='springboard_online',
        sql="""
        INSERT INTO model.Dim_Item (item_name)
        SELECT DISTINCT item
        FROM public.input
        WHERE item IS NOT NULL
          AND item NOT IN (SELECT item_name FROM model.Dim_Item);
        """
    )

    load_dim_practitioner = PostgresOperator(
    task_id='load_dim_practitioner',
    postgres_conn_id='springboard_online',
    sql="""
    -- Insert new practitioners or update team changes
    INSERT INTO model.Dim_Practitioner (
        practitioner_id,
        practitioner_name,
        email,
        contract_type,
        manager_name,
        location_id,
        location_name,
        is_current
    )
    SELECT DISTINCT
        CAST(i."Staff ID" AS INT) AS practitioner_id,
        i."Staff name" AS practitioner_name,
        LOWER(REPLACE(i."Staff name", ' ', '.')) || '@springboard.ca' AS email,
        NULL AS contract_type,
        i.manager AS manager_name,
        loc.location_id,
        loc.location_name,
        TRUE AS is_current
    FROM public.input AS i
    JOIN model.Dim_Location AS loc ON i.location = loc.location_name
    WHERE i."Staff ID" IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM model.Dim_Practitioner AS p
          WHERE p.practitioner_id = CAST(i."Staff ID" AS INT)
            AND p.manager_name = i.manager
            AND p.is_current = TRUE
      );

    -- Update is_current to FALSE for previous records of practitioners who changed teams
    UPDATE model.Dim_Practitioner AS p
    SET is_current = FALSE
    WHERE is_current = TRUE 
      AND EXISTS (
          SELECT 1 
          FROM model.Dim_Practitioner AS o 
          WHERE o.is_current = TRUE 
            AND o.practitioner_id = p.practitioner_id
            AND o.practitioner_dim_id > p.practitioner_dim_id
      );
    """
    )

    # Helper function to get the last date in fact table
    def get_last_fact_date():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="springboard_online")
        result = pg_hook.get_first("SELECT MAX(date) FROM model.Fact_Performance;")
        print(f"result: {result} result[0]: {result[0]}")
        return result[0] if result[0] else datetime(2020, 1, 1)  # default start date

    # Step 4: Load new data into Fact_Performance
    load_fact_performance = PostgresOperator(
        task_id='load_fact_performance',
        postgres_conn_id='springboard_online',
        sql="""
        INSERT INTO model.Fact_Performance (date, practitioner_dim_id, location_id, target_hour, actual_hour, total_billing)
        SELECT
            i."Purchase Date" AS date,
            p.practitioner_dim_id,
            l.location_id,
            COALESCE(SUM(t.target_hour), 0) AS target_hour,
            COALESCE(SUM(i."Actual hour"), 0) AS actual_hour,
            COALESCE(SUM(i.total), 0) AS total_billing
        FROM (
            SELECT "Purchase Date", CAST("Staff ID" AS INT) AS practitioner_id, location, SUM("Actual hour") AS "Actual hour", SUM(total) AS total
            FROM public.input
            WHERE "Purchase Date" > '{{ task_instance.xcom_pull(task_ids="get_last_fact_date") }}'
            GROUP BY "Purchase Date", practitioner_id, location
        ) AS i
        JOIN model.Dim_Practitioner AS p ON i.practitioner_id = p.practitioner_id AND p.is_current = TRUE
        JOIN model.Dim_Location AS l ON i.location = l.location_name
        LEFT JOIN public.target AS t ON i.practitioner_id = CAST(t."Staff ID" AS INT) AND i."Purchase Date" = t.appt_date
        GROUP BY i."Purchase Date", p.practitioner_dim_id, l.location_id;
        """,
        parameters={
            'last_date': "{{ task_instance.xcom_pull(task_ids='get_last_fact_date') }}"
        }
    )

    # Step 5: Load new data into Fact_Appointments
    load_fact_appointments = PostgresOperator(
        task_id='load_fact_appointments',
        postgres_conn_id='springboard_online',
        sql="""
        INSERT INTO model.Fact_Appointments (date, practitioner_dim_id, location_id, item_id, number_appoiments, actual_hour, total_billing)
        SELECT
            i."Purchase Date" AS date,
            p.practitioner_dim_id,
            l.location_id,
            it.item_id,
            COUNT(i.item) AS number_appointments,
            COALESCE(SUM(i."Actual hour"), 0) AS actual_hour,
            COALESCE(SUM(i.total), 0) AS total_billing
        FROM public.input AS i
        JOIN model.Dim_Practitioner AS p ON CAST(i."Staff ID" AS INT) = p.practitioner_id AND p.is_current = TRUE
        JOIN model.Dim_Location AS l ON i.location = l.location_name
        JOIN model.Dim_Item AS it ON i.item = it.item_name
        WHERE i."Purchase Date" > '{{ task_instance.xcom_pull(task_ids="get_last_fact_date") }}'
        GROUP BY i."Purchase Date", p.practitioner_dim_id, l.location_id, it.item_id;
        """,
        parameters={
            'last_date': "{{ task_instance.xcom_pull(task_ids='get_last_fact_date') }}"
        }
    )

    # Task to pull the last date from Fact_Performance
    get_last_fact_date = PythonOperator(
        task_id='get_last_fact_date',
        python_callable=get_last_fact_date
    )

    # Define task dependencies
    [load_dim_location, load_dim_item] >> load_dim_practitioner >> get_last_fact_date >> load_fact_performance >> load_fact_appointments

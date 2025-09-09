import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px


#database connectivity

def create_connection():
    try:
        connecion=pymysql.connect(
            host='localhost',
            user='PoojaSampath',
            password='Jelloo@0813',
            database='SecurePolicePostLogs_db',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connecion
    except Exception as e:
        st.error(f"Database connectivity error:{e}")
        return None
    
#fetch data from database

def fetch_data(query):
    connection =create_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result=cursor.fetchall()
                df=pd.DataFrame(result)
                return df
        finally:
            connection.close()
    else:
        return pd.DataFrame()
    
#StreamLit UI
st.set_page_config(page_title="Secure Check Police Post",layout='wide')

st.title("SecureCheck: Police Check Post Digital Ledger")
st.markdown("Real-time monitoring and insights for law enforcement")

#show Details as table
st.header("Police Logs Overview")
query="SELECT * FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data"
data=fetch_data(query)
st.dataframe(data,use_container_width=True)

#Quick Metrics
st.header("Key Metrics")

col1,col2,col3,col4=st.columns(4)

with col1:
    total_stops=data.shape[0]
    st.metric("Total Police Post Stops",total_stops)
with col2:
    arrests=data[data['stop_outcome'].str.contains("arrest",case=False,na=False)].shape[0]
    st.metric("Total No Of Arrest",arrests)
with col3:
    warnings=data[data['stop_outcome'].str.contains("warning",case=False,na=False)].shape[0]
    st.metric("Total No Of Warnings",warnings)
with col4:
    drug_related=data[data['drugs_related_stop']==1].shape[0]
    st.metric("Drug Related Stops",drug_related)

#Charts
st.header("Visual Insights")
tab1, tab2=st.tabs(["Stops by Violation","Driver Gender Distribution"])
with tab1:
    if not data.empty and 'violation' in data.columns:
        violation_data=data['violation'].value_counts().reset_index()
        violation_data.columns=['Violation','Count']
        fig=px.bar(violation_data,x='Violation',y='Count',title="Stops by violation type",color='Violation')
        st.plotly_chart(fig,use_container_width=True)
    else:
        st.warning("No data available for violation chart")
    
with tab2:
    if not data.empty and 'driver_gender' in data.columns:
        gender_data=data['driver_gender'].value_counts().reset_index()
        gender_data.columns=['Gender','Count']
        fig=px.bar(gender_data,x='Gender',y='Count',title="Driver GenderDistribution")
        st.plotly_chart(fig,use_container_width=True)
    else:
        st.warning("No data available for Driver Gender chart")

#Advanced Queries
st.header("Advanced Insights")
selected_query=st.selectbox("Please select the required condition to run",[
    "What are the top 10 vehicle_Number involved in drug-related stops?",
    "Which vehicles were most frequently searched",
    "Which driver age group had the highest arrest rate?",
    "What is the gender distribution of drivers stopped in each country",
    "Which race and gender combination has the highest search rate?",
    "What time of day sees the most traffic stops?",
    "What is the average stop duration for different violations?",
    "Are stops during the night more likely to lead to arrests?",
    "Which violations are most associated with searches or arrests?",
    "Which violations are most common among younger drivers (<25)?",
    "Is there a violation that rarely results in search or arrest?",
    "Which countries report the highest rate of drug-related stops?",
    "What is the arrest rate by country and violation?",
    "Which country has the most stops with search conducted?"
    
])
query_map={
    "What are the top 10 vehicle_Number involved in drug-related stops?":""" 
    SELECT vehicle_number, COUNT(*) AS drug_related_stop_count
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
WHERE drugs_related_stop = 1
GROUP BY vehicle_number
ORDER BY drug_related_stop_count DESC
LIMIT 10
""",
    "Which vehicles were most frequently searched":"""SELECT vehicle_number,COUNT(*) AS total_searches FROM 
    SecurePolicePostLogs_db.SecurePolicePostLogs_data WHERE search_conducted=1 GROUP BY vehicle_number ORDER BY total_searches DESC LIMIT 10""",
    "Which driver age group had the highest arrest rate?":"""SELECT 
    CASE 
    WHEN driver_age<18 THEN '<18' 
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    ELSE '>60'
    END AS driver_age_group, AVG(is_arrested)*100.0 AS arrest_rate FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data GROUP BY driver_age_group ORDER BY arrest_rate DESC LIMIT 1""",
    "What is the gender distribution of drivers stopped in each country":"""SELECT country_name,driver_gender,COUNT(*) AS total_stops, ROUND((COUNT(*)*100.0/SUM(COUNT(*)) OVER (PARTITION BY country_name)),2 )AS percentage FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
     GROUP BY country_name,driver_gender 
      ORDER BY country_name,total_stops DESC""",
    "Which race and gender combination has the highest search rate?":"""SELECT driver_race,driver_gender,AVG(search_conducted)*100 AS search_rate_percentage FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
    GROUP BY driver_race,driver_gender
    ORDER BY search_rate_percentage DESC
    LIMIT 1""",
    "What time of day sees the most traffic stops?":"""SELECT 
    CASE
    WHEN HOUR(time_stamp) BETWEEN 5 AND 11 THEN 'Morning(5AM - 11AM)'
    WHEN HOUR(time_stamp) BETWEEN 12 AND 16 THEN 'Afternoon(12PM - 4PM)'
    WHEN HOUR(time_stamp) BETWEEN 17 AND 20 THEN 'Evening(5PM - 8PM)'
    ELSE 'Night(9PM - 4AM)'
    END AS time_of_day,COUNT(*) AS total_stops FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
    GROUP BY time_of_day
    ORDER BY total_stops DESC
    LIMIT 1""",
"What is the average stop duration for different violations?":"""SELECT violation,
AVG(
CASE stop_duration
WHEN '0-15 Min' THEN 7.5
WHEN '16-30 Min' THEN 23
WHEN '30+ Min' THEN 45
END
) AS avg_stop_duration_minutes FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY violation
ORDER BY avg_stop_duration_minutes desc""",
"Are stops during the night more likely to lead to arrests?":"""SELECT
CASE 
WHEN HOUR(time_stamp) BETWEEN 21 AND 23
OR HOUR(time_stamp) BETWEEN 0 AND 4 THEN 'Night (9PM-4AM)'
ELSE 'Daytime (5AM-8PM)'
END AS time_period, COUNT(*) AS total_stops,
SUM(is_arrested) AS total_arrests,
ROUND(SUM(is_arrested)*100.0/COUNT(*),2) AS arrest_rate_percentage FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY time_period
""",
    "Which violations are most associated with searches or arrests?":"""
SELECT violation,ROUND(AVG(search_conducted)*100,2) AS search_rate_percentage,ROUND(AVG(is_arrested)*100,2) AS arrest_rate_percentage
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY violation
ORDER BY search_rate_percentage DESC,
arrest_rate_percentage DESC""",
    "Which violations are most common among younger drivers (<25)?":"""
SELECT 
    violation,
    COUNT(*) AS total_stops
FROM SecurePolicePostLogs_data
WHERE driver_age < 25
GROUP BY violation
ORDER BY total_stops DESC
LIMIT 10
""",
    "Is there a violation that rarely results in search or arrest?":"""
SELECT 
    violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS searches,
    SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS arrests,
    ROUND(100.0 * SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS search_rate,
    ROUND(100.0 * SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY violation
HAVING search_rate < 5 AND arrest_rate < 5  -- Rarely results in search or arrest
ORDER BY total_stops DESC;
 """,
    "Which countries report the highest rate of drug-related stops?":"""
SELECT country_name,
       SUM(drugs_related_stop) * 1.0 / COUNT(*) AS drug_related_stop_rate
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY country_name
ORDER BY drug_related_stop_rate DESC
LIMIT 1
    """,
    "What is the arrest rate by country and violation?":"""
SELECT 
    country_name,
    violation,
    COUNT(*) AS total_stops,
    SUM(is_arrested) AS total_arrests,
    ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) AS arrest_rate
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY country_name, violation
ORDER BY country_name, arrest_rate DESC;
    """,
    "Which country has the most stops with search conducted?":"""
SELECT 
    country_name,
    COUNT(*) AS search_stops
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
WHERE search_conducted = 1
GROUP BY country_name
ORDER BY search_stops DESC
LIMIT 1
"""
                                                                            


}
if st.button("Run Query"):
    result=fetch_data(query_map[selected_query])
    if not result.empty:
        st.write(result)
    else:
        st.warning("No results found for the selected query.")








#Complex Queries
st.header("Complex Queries")
selected_query=st.selectbox("Please select the required condition to run",[
    "Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)",
    "Driver Violation Trends Based on Age and Race (Join with Subquery)",
    "Time Period Analysis of Stops (Joining with Date Functions) , Number of Stops by Year,Month, Hour of the Day",
    "Violations with High Search and Arrest Rates (Window Function)",
    "Driver Demographics by Country (Age, Gender, and Race)",
    "Top 5 Violations with Highest Arrest Rates"
    
])
query_map_complex={
    "Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)":"""
        SELECT
    country_name,
    year,
    total_stops,
    total_arrests
FROM
    (
        SELECT
            country_name,
            EXTRACT(YEAR FROM stop_date) AS year,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests
        FROM
            SecurePolicePostLogs_db.SecurePolicePostLogs_data
        GROUP BY
            country_name, year
    ) AS yearly_stats
ORDER BY
    country_name, year
""",
    "Driver Violation Trends Based on Age and Race (Join with Subquery)":"""
SELECT
    d.driver_age,
    d.driver_race,
    v.violation_count
FROM
    SecurePolicePostLogs_db.SecurePolicePostLogs_data AS d
JOIN
    (
        SELECT
            driver_age,
            driver_race,
            COUNT(*) AS violation_count
        FROM
            SecurePolicePostLogs_db.SecurePolicePostLogs_data
        GROUP BY
            driver_age, driver_race
    ) AS v
ON d.driver_age = v.driver_age AND d.driver_race = v.driver_race
GROUP BY
    d.driver_age, d.driver_race, v.violation_count
ORDER BY
    d.driver_age, d.driver_race;
    """,
"Time Period Analysis of Stops (Joining with Date Functions) , Number of Stops by Year,Month, Hour of the Day":
    """
SELECT
    s.stop_date,
    t.year,
    t.month,
    t.hour,
    t.total_stops
FROM
    SecurePolicePostLogs_db.SecurePolicePostLogs_data AS s
JOIN
    (
        SELECT
            EXTRACT(YEAR FROM stop_date) AS year,
            EXTRACT(MONTH FROM stop_date) AS month,
            EXTRACT(HOUR FROM stop_time) AS hour,
            COUNT(*) AS total_stops
        FROM
            SecurePolicePostLogs_db.SecurePolicePostLogs_data
        GROUP BY
            year, month, hour
    ) AS t
ON EXTRACT(YEAR FROM s.stop_date) = t.year
   AND EXTRACT(MONTH FROM s.stop_date) = t.month
   AND EXTRACT(HOUR FROM s.stop_time) = t.hour
ORDER BY
    t.year, t.month, t.hour
""",
    "Violations with High Search and Arrest Rates (Window Function)":""" 
SELECT 
    violation,
    AVG(search_conducted) * 100 AS search_rate,
    AVG(is_arrested) * 100 AS arrest_rate
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY violation
ORDER BY search_rate DESC, arrest_rate DESC
LIMIT 5
""",
    "Driver Demographics by Country (Age, Gender, and Race)":"""
SELECT 
    country_name,
    AVG(driver_age) AS average_age,
    driver_gender,
    driver_race,
    COUNT(*) AS total_drivers
FROM SecurePolicePostLogs_db.SecurePolicePostLogs_data
GROUP BY country_name, driver_gender, driver_race
ORDER BY country_name;
    """,
    "Top 5 Violations with Highest Arrest Rates":""" 
SELECT 
    violation,
    AVG(is_arrested) * 100 AS arrest_rate
FROM SecurePolicePostLogs_data
GROUP BY violation
ORDER BY arrest_rate DESC
LIMIT 5"""
}


if st.button("Run Advanced Query"):
    result=fetch_data(query_map_complex[selected_query])
    if not result.empty:
        st.write(result)
    else:
        st.warning("No results found for the selected query.")


st.header("Stop outcome and Violation Prediction")
st.markdown("Fill in the details below to get a prediction of stop outcome based on existing data")
st.header("Add new police log & predict Outco and Violation")

#Input from all fields
with st.form("new_log_form"):
    stop_date=st.date_input("Stop Date")
    stop_time=st.time_input("Stop Time")
    country_name=st.text_input("Country Name")
    driver_gender=st.selectbox("Driver Gender",["M","F"])
    driver_age=st.number_input("Driver Age",min_value=16,max_value=100,value=27)
    driver_race=st.text_input("Driver Race")
    search_conducted=st.selectbox("is search conducted?",["0","1"])
    search_type=st.text_input("Search Type")
    drugs_related_stop=st.selectbox("Was it drug related?",["0","1"])
    stop_duration=st.selectbox("Stop Duration",data['stop_duration'].dropna().unique())
    vehicle_number=st.text_input("Vehicle Number")
    timestamp=pd.Timestamp.now()

    submitted=st.form_submit_button("Predict Stop Outcome & Violation")

    if submitted:
        filtered_data = data[
        (data['driver_gender'] == driver_gender)&
        (data['driver_age'] == driver_age)&
        (data['search_conducted'] == int(search_conducted))&
        (data['stop_duration'] == stop_duration)&
        (data['drugs_related_stop'] == int(drugs_related_stop))&
        (data['vehicle_number'] == vehicle_number)
          ]
    
    #predict stop outcome
        if not filtered_data.empty:
            predicted_outcome = filtered_data['stop_outcome'].mode()[0]
            predicted_violation = filtered_data['violation'].mode()[0]
        else:
            predicted_outcome="warning" #default fallback
            predicted_violation="speeding" #default fallback

#summary

        search_text="A search was conducted" if int(search_conducted) else "No search was conducted"
        drug_text="was drug-related" if int(drugs_related_stop) else "was not drug-related"

        st.markdown(f"""
           **Prediction Summary**
            -**Predicted Violation:**{predicted_violation}
            -**Predicted Stop Outcome:**{predicted_outcome}
            A {driver_age}-year-old {driver_gender} driver in {country_name} was stopped at {stop_time.strftime('%I:%M %p')} on {stop_date} . 
            {search_text}, and the stop {drug_text},
            stop_duration:**{stop_duration}**,
            Vehicle Number: **{vehicle_number} **
            """
            )


    

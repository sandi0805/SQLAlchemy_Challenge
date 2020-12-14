#%%
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from flask import Flask

#%%
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

#%%
app = Flask(__name__)

#%%
@app.route("/")
def welcome():
    return (
        f"Welcome to the Hawaii Climate Analysis API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start_date&gt; <em>(enter the start_date in the YYYY-MM-DD format)</em><br/>"
        f"/api/v1.0/&lt;start_date&gt;/&lt;end_date&gt; <em>(enter the start_date and end_date in the YYYY-MM-DD format)</em><br/>"
    )

#%%
@app.route('/api/v1.0/precipitation')
def prcp():
    
    conn = engine.connect()
    
    query = f'''
        SELECT 
            date,
            AVG(prcp) as avg_prcp
        FROM
            measurement
        WHERE
            date >= (SELECT DATE(MAX(date),'-1 year') FROM measurement)
        GROUP BY
            date
        ORDER BY 
            date
    '''

    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.read_sql(query, conn)
    
    # Convert the date column to date
    prcp_df['date'] = pd.to_datetime(prcp_df['date'])
        
    # Sort the dataframe by date
    prcp_df.sort_values('date')
    
    prcp_json = prcp_df.to_json(orient='records', date_format = 'iso')
    
    conn.close()
    
    return prcp_json


@app.route('/api/v1.0/stations')
def station():
    conn = engine.connect()
    
    query = "SELECT station FROM measurement"
    station_df = pd.read_sql(query, conn)
    station_json = station_df.to_json(orient='records')
    
    conn.close()
    
    return station_json

#%%
@app.route('/api/v1.0/tobs')
def temp():
    conn = engine.connect()
    
    # Try not to hardcode the most active station. Instead look up the most active station.
    query = "SELECT date, tobs FROM measurement WHERE station = 'USC00519281'"
    tobs_df = pd.read_sql(query, conn)
    tobs_json = tobs_df.to_json(orient = 'records', date_format='iso')

    conn.close()

    return tobs_json

#%%
@app.route('/api/v1.0/<start_date>')
def start(start_date):
    conn = engine.connect()
    query  = f'''
        SELECT	
            MIN(tobs),
            MAX(tobs),
            AVG(tobs)
        FROM	
            measurement
        WHERE
            date >= '{start_date}'
    '''
    data_df = pd.read_sql(query, conn)
    data_json = data_df.to_json(orient='records', date_format='iso')

    conn.close()

    return data_json 
#%%
@app.route('/api/v1.0/<start_date>/<end_date>')
def start_end(start_date, end_date):
    conn = engine.connect()
    
    query = f'''
        SELECT	
            MIN(tobs) AS min_tobs,
            MAX(tobs) AS max_tobs,
            AVG(tobs) AS avg_tobs
        FROM	
            measurement
        WHERE
            date BETWEEN '{start_date}' AND '{end_date}'
    '''         
    data_df = pd.read_sql(query, conn)
    data_json = data_df.to_json(orient='records', date_format='iso')
    
    conn.close()

    return data_json 

#%%
if __name__ == '__main__':
    app.run(debug=True)    
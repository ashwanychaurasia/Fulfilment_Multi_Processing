import mysql.connector
from datetime import datetime, timedelta
import time
import pandas as pd
import os
from multiprocessing import Pool, cpu_count
import mysql.connector
from datetime import datetime, timedelta
import time
import pandas as pd
import os
from multiprocessing import Pool, cpu_count
from openpyxl import load_workbook
import numpy as np
import pandas as pd
import os
import glob

working_directory = r"D:\Ashwany\Fulfilment"

db_config = {
   'host': "172.31.2.82",
  'user': 'de_ss_comon',
    'password': 'Con@%^&(*657',
    'database': 'Scadi_Shipments',
    'port' : 6033}

def fetch_data(args):
    start_datetime, end_datetime = args
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = """
        select
        distinct ss.airwaybill_number as 'AWB',
        
          date(ss.inscan_date) as 'Inscan_Date',
          case when ss.is_cod = 'Y' then 'CoD'
          when ss.is_cod = 'N' then 'Prepaid' end as 'Payment_Mode',
          ss.shipper_id as 'Shipper_ID',
          case when ss.shipper_id in (161,30762) then 'Meesho' when ss.shipper_id in (170,1937) then 'Flipkart' end as 'Shipper_Name',
          msc.center_code as 'DC',
          mdmc.city_name as 'Destination_City',
          mdms.state_code as 'Destination_State',
          mdmr.region_name as 'Destination_Region',
          case when ss.current_status_id = 21 then 'Delivered'
          when ss.current_status_id = 22 then 'Returned'
          when ss.current_status_id = 20 then 'Un_Delivered'
          else 'In_Progress' end as 'Shipment_Status',
          #ss.collectable_value as 'Collectable_Value',
          (select date(min(sh.updated_on)) from shipment_instructions_history sh where ss.id = sh.shipment_id and sh.instruction_id =21) as 'RTO'
          
          from shipments ss
          left join mdm_service_centers msc on msc.id = ss.dest_sc_id
          left join mdm_service_center_hierarchy_crossref msch on msch.service_center_id = ss.dest_sc_id
          left join mdm_cities mdmc on mdmc.id = msch.city_id
          left join mdm_states mdms on mdms.id = msch.state_id
          left join mdm_regions mdmr on mdmr.id = msch.region_id
          
where ss.inscan_date >= %s
and ss.inscan_date < %s
and ss.shipper_id in (161,30762,170,1937)
and ss.product_type_id not in (4,7)
#group by 1,2,3,4,5,6,7,8,9
"""

        cursor.execute(query, (start_datetime, end_datetime))
        result = cursor.fetchall()

        if not result:
            print(f"No data found for {start_datetime} to {end_datetime}")
            return None
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)

        return df

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=15)
    start_date = end_date - timedelta(days=15)

    time_interval = timedelta(hours=1)  

    date_ranges = [(start_date + i * time_interval, start_date + (i + 1) * time_interval) for i in range(int((end_date - start_date).total_seconds() / 3600))]
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(fetch_data, date_ranges)
    daily_dataframes = {}
    for df in results:
        if df is not None:
            date = df['Inscan_Date'].iloc[0]
            if date not in daily_dataframes:
                daily_dataframes[date] = df
            else:
                daily_dataframes[date] = pd.concat([daily_dataframes[date], df])
    for date, df in daily_dataframes.items():
        csv_filename = os.path.join(working_directory, f"Raw_Data_{date.strftime('%Y-%m-%d')}.csv")
        df.to_csv(csv_filename, index=False)
        print(f"Data exported to {csv_filename}")
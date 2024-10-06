import azure.functions as func
import logging
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
import psycopg2
from ticketmaster.params import params
from ticketmaster.constant import prediction_location_rename_dict
import time



def main(myTimer: func.TimerRequest) -> None:
    # def get_coordinates(address):
    #     # url = f"https://geocode.maps.co/search?q={address}&api_key=667279b81cc09213794345bkr54c08e"
    #     # url = f"https://nominatim.openstreetmap.org/search?addressdetails=1&q={address}&format=jsonv2&limit=1"
    #     url = f"https://us1.locationiq.com/v1/search?key=pk.ea77c133fe66dcecd00141928451c03a&q={address}&format=json&"

    #     try:
    #         response = requests.get(url)
    #         response.raise_for_status()  # Check if the request was successful
    #         response_json = response.json()  # Attempt to parse JSON
    #         if response_json:
    #             return {
    #                 "latitude": float(response_json[0]["lat"]),
    #                 "longitude": float(response_json[0]["lon"])
    #             }
    #         else:
    #             return None
    #     except requests.exceptions.RequestException as e:
    #         print(f"Request error for {address}: {e}")
    #         return None
    #     except ValueError as e:
    #         print(f"JSON decode error for {address}: {e}")
    #         return None

    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")

    api_key = "60ZqoOTbOfCIimOeUygumt6V2fhPU4sT"
    endpoint = "https://app.ticketmaster.com/discovery/v2/events.json"
    events_list = []
    current_date = datetime.now()
    end_date = current_date+ relativedelta(months=6)

    locations= [
        "Oslo"
        ,"Bergen", "Stavanger", "Trondheim", "Fredrikstad",
        # "Drammen", "Skien", "Kristiansand", "Ålesund", "Tønsberg",
        # "Moss", "Sandefjord", "Haugesund", "Arendal", "Bodø","Tromsø", 
        # "Hamar", "Larvik", "Halden", "Jessheim",
        # "Kongsberg", "Molde", "Harstad", "Lillehammer", "Ski",
        # "Horten", "Gjøvik", "Mo i Rana", "Kristiansund", "Hønefoss",
        # "Alta", "Elverum", "Askim", "Leirvik", "Osøyro",
        # "Narvik", "Grimstad", "Drøbak", "Nesoddtangen", "Steinkjer",
        # "Bryne", "Kongsvinger", "Egersund", "Brumunddal", "Mandal",
        # "Ås", "Førde", "Levanger", "Arna", "Mosjøen",
        # "Notodden", "Florø", "Namsos", "Lillesand", "Holmestrand",
        # "Raufoss", "Hammerfest", "Ørsta", "Melhus", "Volda",
        # "Eidsvoll", "Knarvik", "Spydeberg", "Fauske", "Flekkefjord",
        # "Sandnessjøen", "Ulsteinvik", "Stavern"
    ]

    while current_date < end_date:
    # Calculate the start and end dates for the current month
        start_date = current_date.strftime("%Y-%m-%d")
        next_month = current_date+ relativedelta(months=2)
        end_date_month = next_month - timedelta(days=next_month.day)
        end_date_str = end_date_month.strftime("%Y-%m-%d")

        # for location, loc_data in weather_locations.items():
        for location in locations:
            page_number = 0
            total_pages = 0
            while page_number <= total_pages:
                parameters = {
                    "countryCode": "NO",
                    "city": location,
                    "apikey": api_key,
                    'locale': '*',
                    'size':200,
                    'page':page_number,
                    'startDateTime': start_date + 'T00:00:00Z',
                    'endDateTime': end_date_str + 'T23:59:59Z'
                }
                response = requests.get(endpoint, params=parameters)

                if response.status_code == 200:
                    json_data = response.json()
                    events_data = json_data.get("_embedded", {}).get("events", [])
                    if events_data:
                        total_pages = json_data.get("page", {}).get("totalPages")
                        for event_data in events_data:
                            venue_name = event_data.get("_embedded", {}).get("venues", [])[0].get("name")
                            longitude = event_data.get("_embedded", {}).get("venues", [])[0]["location"]["longitude"]
                            latitude = event_data.get("_embedded", {}).get("venues", [])[0]["location"]["latitude"]
                            audience_type = "Under 18, 18-30, 30-45, 45-60, 60+" if (event_data.get("classifications", [])[0].get("family") == "true" ) else "18-30, 30-45, 45-60, 60+"
                            event = {
                                "city": location,
                                "event_category": event_data.get("classifications", [])[0].get("segment", {}).get("name"),
                                "location": venue_name,
                                "name": event_data.get("name", ""),
                                "start_date": event_data.get("dates", {}).get("start", {}).get("localDate"),
                                "audience_type": audience_type,
                                "latitude":latitude,
                                "longitude": longitude
                            }
                            events_list.append(event)
                    else:
                        break
                    logging.info(f"events found {len(events_list)} for {location} till month {end_date_str}")
                    page_number += 1
                else:
                    # print("Errors:", response.status_code)
                    break
        # Move to the next month
        current_date = next_month
    df = pd.DataFrame(events_list)
    df.drop_duplicates(inplace=True)
    df['start_date'] = pd.to_datetime(df['start_date'])
    # df.to_csv("test_csv/ticketmaster_orignal_added_location.csv", index=False)
    df_sorted = df.sort_values(by=['city', 'event_category', 'location', 'name', 'start_date','audience_type'])
    # df_sorted.to_csv("test_csv/sorted.csv",index=False)
    


    df_sorted['group'] = (df_sorted['start_date'] - df_sorted['start_date'].shift(1)).dt.days.ne(1).cumsum()
    # Step 4: Group by the newly created 'group' and aggregate to get the start and end dates
    transformed_df = df_sorted.groupby(['city', 'event_category', 'location', 'name','audience_type','latitude','longitude', 'group']).agg(
                                                                                                        city =('city', 'first'),
                                                                                                        event_category =('event_category', 'first'),
                                                                                                        location =('location', 'first'),
                                                                                                        name =('name', 'first'),
                                                                                                        start_date=('start_date', 'first'),
                                                                                                        end_date=('start_date', 'last'),
                                                                                                        latitude=('latitude', 'first'),
                                                                                                        longitude=('longitude', 'first'),
                                                                                                        audience_type=('audience_type', 'first')
                                                                                                        ).reset_index(drop=True)
    transformed_df.loc[transformed_df['start_date'] == transformed_df['end_date'],'end_date']= None
    transformed_df['location'] = transformed_df['location'].str.strip('"')

    transformed_df['location'] = transformed_df['location'].apply(lambda x: prediction_location_rename_dict.get(x, x))
    transformed_df['name'] = transformed_df['name'].apply(lambda x: x[:50])

    location_coordinates = transformed_df[['city', 'location', 'latitude','longitude']].drop_duplicates()
    # location_coordinates.to_csv('location_coordinates.csv')
    

# add city if not already in our database ------------------------------------------------------------------------------------------
    all_cities_query = """SELECT "name" FROM public."accounts_city";"""
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(all_cities_query)
            conn.commit()
            columns = [col[0] for col in cursor.description]
            all_cities = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # logging.info(all_cities)
    unique_cities_ticketmaster= transformed_df['city'].unique()

    for city in unique_cities_ticketmaster:
        if not any(item['name'] == city for item in all_cities):
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO public.accounts_city (id,name,created_at) VALUES (gen_random_uuid(),%s,%s)", [city,datetime.now()])
                    conn.commit()
                    logging.info(f"city created {city}")
        else:
        #     logging.info(f"City already exists: {city}")
            continue

# # add category if not already in our database------------------------------------------------------------------------------------------
    all_categories_query = """SELECT "name" FROM public."Predictions_eventcategory";"""
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(all_categories_query)
            conn.commit()
            columns = [col[0] for col in cursor.description]
            all_categories = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # print(all_categories)
    unique_category_ticketmaster = transformed_df['event_category'].unique()

    for event_category in unique_category_ticketmaster:
        if not any(item['name'] == event_category for item in all_categories):
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('INSERT INTO public."Predictions_eventcategory" (id,name,description,created_at) VALUES (gen_random_uuid(),%s,null,%s)', [event_category,datetime.now()])
                    conn.commit()
                    # logging.info(f"{event_category} category created")
        else:
        #     logging.info(f" Event category already exists:{event_category} ")
            continue

# #---add prediction location if not already in our database----------------------------------------------------------------------------
    all_locations_query = """SELECT name FROM public."Predictions_location";"""
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(all_locations_query)
            conn.commit()
            columns = [col[0] for col in cursor.description]
            all_locations = [dict(zip(columns, row)) for row in cursor.fetchall()]

    unique_locations_ticketmaster = transformed_df[['city', 'location','latitude','longitude']].drop_duplicates()



    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            for index, row in unique_locations_ticketmaster.iterrows():
                location_name = row['location']
                city_name = row['city']
                latitude= row['latitude']
                longitude= row['longitude']
                cursor.execute('SELECT id FROM "accounts_city" WHERE name = %s', [city_name])
                city_id = cursor.fetchone()
                # Check if the location already exists in the database
                cursor.execute('SELECT name FROM "Predictions_location" WHERE name = %s AND cities_id= %s', [location_name,city_id])
                existing_location = cursor.fetchone()
                if existing_location:
                    cursor.execute('UPDATE public."Predictions_location" SET latitude=%s, longitude=%s, created_at = %s where latitude is null and longitude is null and cities_id=%s and name=%s',
                                   [latitude,longitude,datetime.now(),city_id,location_name])
                    # logging.info(f'updated {location_name} with lat: {latitude} and long: {longitude}')
                if not existing_location:
                    cursor.execute('SELECT id FROM public."accounts_city" WHERE name = %s', [city_name])
                    city_id = cursor.fetchone()

                    if city_id:
                        cursor.execute('INSERT INTO public."Predictions_location" (id, name, cities_id, created_at,latitude,longitude) VALUES (gen_random_uuid(), %s, %s, %s,%s,%s)',
                                    [location_name, city_id[0], datetime.now(),latitude, longitude])
                        logging.info(f"Location created: {location_name}")
                    else:
                        logging.info(f"City '{city_name}' does not exist. Skipping location creation for {location_name}")
                else:
                #     logging.info(f"Location '{location_name}' already exists.")
                    continue
     
    grouped_rows = transformed_df.groupby(['name', 'event_category', 'location', 'start_date'])

    event_insert_values = []
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            for (name, event_category_name, location_name_ticketmaster, start_date), group in grouped_rows:
                # if pd.isnull(row['end_date']):  # Check for NaT
                #     row['end_date'] = None
                # else:
                #     row['end_date'] = row['end_date']
                # Fetch category ID
                cursor.execute('SELECT id FROM "Predictions_eventcategory" WHERE name = %s', [event_category_name])
                category_instance = cursor.fetchone()
                if category_instance is None:
                    # logging.error(f"Category '{event_category_name}' not found")
                    continue
                
                # Fetch location ID
                cursor.execute('SELECT id FROM "Predictions_location" WHERE name = %s', [location_name_ticketmaster])
                location_instance = cursor.fetchone()
                if location_instance is None:
                    # logging.error(f"Location '{location_name_ticketmaster}' not found")
                    continue
                
                # Check if event already exists
                event_exists_query = """
                    SELECT name 
                    FROM public."Events" 
                    WHERE name = %s 
                    AND event_category_id = %s
                    AND location_id = %s 
                    AND start_date = %s 
                """
                cursor.execute(event_exists_query, [name, category_instance[0], location_instance[0], start_date])
                existing_event = cursor.fetchone()
                

                # if not existing_event:

                #     # Bulk insert all rows for this unique combination
                #     insert_event_query = """
                #         INSERT INTO public."Events" (id, name, event_category_id, event_size, location_id, audience_type, is_sold_out, start_date, end_date,created_at) 
                #         VALUES (gen_random_uuid(), %s, %s, 'Unknown', %s, %s, FALSE, %s, %s,%s)
                #     """
                #     values = [(name, category_instance[0], location_instance[0], row['audience_type'], start_date, None if pd.isnull(row['end_date']) else row['end_date'],datetime.now()) for _, row in group.iterrows()]
                #     cursor.executemany(insert_event_query, values)
                #     conn.commit()
                # # logging.info(f"{name} Event created")


                if not existing_event:
                    # Prepare values for bulk insert
                    for _, row in group.iterrows():
                        end_date = None if pd.isnull(row['end_date']) else row['end_date']
                        event_insert_values.append(
                            (name, category_instance[0], location_instance[0], row['audience_type'], start_date, None if pd.isnull(row['end_date']) else row['end_date'], datetime.now())
                        )

            # Bulk insert all rows for unique combinations
            # pd.DataFrame(event_insert_values).to_csv("test.csv")
            if event_insert_values:
                insert_event_query = """
                    INSERT INTO public."Events" (id, name, event_category_id, event_size, location_id, audience_type, is_sold_out, start_date, end_date, created_at) 
                    VALUES (gen_random_uuid(), %s, %s, 'Unknown', %s, %s, FALSE, %s, %s, %s)
                """
                cursor.executemany(insert_event_query, event_insert_values)
                conn.commit()
                logging.info(f'{len(event_insert_values)} events inserted')
    
    city_restaurant_pairs = {
    'Oslo': 'Oslo Torggata',
    'Fredrikstad': 'Fredrikstad',
    'Bergen': 'Bergen',
    'Stavanger': 'Stavanger',
    'Trondheim': 'Trondheim'
    }

    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            for city, restaurant in city_restaurant_pairs.items():
                query = f"""
                        INSERT INTO public."Events_restaurants" (events_id, restaurant_id)
                        SELECT e.id AS events_id, ar.id AS restaurant_id
                        FROM public."Events" e
                        JOIN public."Predictions_location" pl ON e.location_id = pl.id
                        JOIN public."accounts_city" ac ON pl.cities_id = ac.id
                        JOIN public.accounts_restaurant ar ON ar.city = ac.name
                        WHERE ac.name = '{city}' AND ar.name = '{restaurant}'
                        ON CONFLICT (events_id, restaurant_id) DO NOTHING;
                    """
                cursor.execute(query)
                inserted_count = cursor.rowcount
                logging.info(f"Inserted {inserted_count} event(s) for city: '{city}' and restaurant: '{restaurant}'.")
                conn.commit()
    # update existing locations
    # with psycopg2.connect(**params) as conn:
    #     with conn.cursor() as cursor:
    #         new_df = pd.read_csv('final_test.csv')
    #         for index,row in new_df.iterrows():
    #             cursor.execute('UPDATE public."Predictions_location" SET latitude=%s, longitude=%s where latitude is null and longitude is null and name=%s',
    #                                 [row['latitude'],row['longitude'],row['index']])
    #             conn.commit()

#search locations with no coordinates

    # with psycopg2.connect(**params) as conn:
    #     with conn.cursor() as cursor:
    #         without_coordinates_query ='SELECT pl.name as location ,ac.name as city FROM "Predictions_location" pl JOIN accounts_city ac on pl.cities_id = ac.id where pl.latitude is null' 
    #         # locations_without_coordinates = cursor.fetchall()
    #         cursor.execute(without_coordinates_query)
    #         conn.commit()
    #         columns = [col[0] for col in cursor.description]
    #         locations_without_coordinates = [dict(zip(columns, row)) for row in cursor.fetchall()]
    #         no_coordinates_df = pd.DataFrame(locations_without_coordinates)
    #         logging.info(len(no_coordinates_df))
    #         place_coords= {}
    #         for index,row in no_coordinates_df.iterrows():
    #             coords = get_coordinates(f'{row["location"]},{row["city"]},norway')
    #             logging.info(f"{row['location']}, corordinates are {coords}")
    #             if coords:
    #                 place_coords[row['location']] = coords
    #             time.sleep(1)

    #         df = pd.DataFrame.from_dict(place_coords, orient='index')
    #         df.reset_index(inplace=True)
    #         df.to_csv("final_test.csv")
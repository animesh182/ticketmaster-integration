## Fetching events from ticketmaster and adding to our database
---
## Documentation
After getting the Api Key for Ticketmaster, we need to follow these steps inorder to import the city, venues and events into our database

### Step 1. Specify Cities to fetch events:
```
For Example:
    locations= [
        "Oslo","Bergen", "Stavanger","Trondheim", "Fredrikstad",....
    ]
```

### Step 2. Pass the parameters for api_endpoint
```
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
```
### Step 3. Workaround for ticketmaster rate limiting
Since ticketmaster only allows us to fetch max of 200 events upto 5 pages ie: 1000 events.We need to do the followin
1. Only fetch events for 2 months at a time for a city so that we do not exceed the rate limits.
2. get the total_pages from the response to fetch events until that page, not more not less.
    ```
    json_data = response.json()
    total_pages = json_data.get("page", {}).get("totalPages")
    ```
    we then fetch the data

### Step 3. Fetching Events
1. **we pass the parameters and fetch the events**
    ```
    response = requests.get(endpoint, params=parameters)
    if response.status_code == 200:
        json_data = response.json()
        events_data = json_data.get("_embedded", {}).get("events", [])
    ```
2. **Transforming the data**
   
    Here, after we fetch the json data, We are going to get relevant data.
   ```
    for event_data in events_data:
        venue_name = event_data.get("_embedded", {}).get("venues", [])[0].get("name")
        audience_type = "Under 18, 18-30, 30-45, 45-60, 60+" if (event_data.get("classifications", [])[0].get("family") == "true" ) else "18-30, 30-45, 45-60, 60+"
        event = {
            "city": location,
            "event_category": event_data.get("classifications", [])[0].get("segment", {}).get("name"),
            "location": venue_name,
            "name": event_data.get("name", ""),
            "start_date": event_data.get("dates", {}).get("start", {}).get("localDate"),
            "audience_type": audience_type,
        }
        events_list.append(event)
   ```
    now we have data like tihs

    | city | event_category | location | name   | start_date | audience_type |
    |------|----------------|----------|--------|------------|---------------|
    | Oslo | concert        | Ulleval  | AURORA | 2024-02-01 |               |
    | Oslo | concert        | Ulleval  | AURORA | 2024-02-02 |               |
    | Oslo | concert        | Ulleval  | Metalica | 2024-02-05 |               |
    | Oslo | Musical        | Ulleval  | DOOM | 2024-02-07 |               |
    | Oslo | Musical        | Ulleval  | DOOM | 2024-02-08 |               |

    Still we do not have a **start_date** and **end_date** just the dates, we need to look for events with everything same except the start_date and if they are incrementing by one day or not so we can get data like this

    | city | event_category | location | name   | start_date | end_date   | audience_type |
    |------|----------------|----------|--------|------------|------------|---------------|
    | Oslo | concert        | Ulleval  | AURORA | 2024-02-01 | 2024-02-02 |               |
    | Oslo | concert        | Ulleval  | Metalica | 2024-02-05 | None       |               |
    | Oslo | Musical        | Ulleval  | DOOM | 2024-02-07 | 2024-02-08       |               |

### Step 4. Inserting Events to DB
Since we already have some cities,venues,categories and events in our DB, We should check if they exist or not and just insert the new ones.

1. **Add city if not already in our database**
   
   fetch all cities
   ``` 
   "SELECT "name" FROM public."accounts_city"
   ```
   insert if not already in DB
   ```
    for city in unique_cities_ticketmaster:
        if not any(item['name'] == city for item in all_cities):
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO public.accounts_city (id,name,
                    created_at) VALUES (gen_random_uuid(),%s,%s)", [city,datetime.now()])
                    conn.commit()
                    logging.info(f"city created {city}")
        else:
        #     logging.info(f"City already exists: {city}")
            continue
   ```

2. **Add Prediction locations**
   
   First we need to rename some of the locations as we already have them in our database.
   ```
   transformed_df['location'] = transformed_df['location'].apply(
    lambda x: prediction_location_rename_dict.get(x, x)
    )
   ```
   - Referenced file: [constant.py](ticketmaster/constant.py)
    ---

   then we get unique locations from ticketmaster and locations that we already have in DB

   ```
   all_cities_query = """SELECT "name" FROM public."accounts_city";"""
   unique_cities_ticketmaster= transformed_df['city'].unique()
   and do the same as the reference step: 4.1
   ```

3. **Add Event categories**
   
   Do the same as reference step: 4.1 for Predictions_eventcategory

4. **Add Events**
   
   - check if events exist in our DB for that location and City
   - If not insert the events
---
---
  
Referenced file: [run.py](ticketmaster/run.py)
## End of Documentation





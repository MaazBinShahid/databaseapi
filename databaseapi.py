

from flask import Flask, request, jsonify, Response, send_file
import asyncio
import asyncpg
from datetime import datetime
import pandas as pd
import io
app = Flask(__name__)

# Database connection settings
DATABASE_URL = 'postgresql://cocihomesdb_owner:j78CsNBHawmA@ep-damp-water-a5i4eugu.us-east-2.aws.neon.tech/cocihomesdb?sslmode=require'






BATCH_SIZE = 100  # Process 100 rows at a time
CONCURRENT_LIMIT = 5  # Limit concurrent inserts

semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)



async def insert_data_batch(batch):
    async with semaphore:
        try:
            conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)  # Increase timeout if necessary

            # Batch insert data into the database
            await conn.executemany('''
                INSERT INTO properties(owner_name, address, phone_number) VALUES($1, $2, $3)
            ''', [(data['owner_name'], data['address'], data['phone_number']) for data in batch])
            
            # Close the connection
            await conn.close()
            print(f"Batch insert successful: {BATCH_SIZE} leads uploaded.")
            return {"status": "success"}

        except Exception as e:
            return {"status": "error", "message": str(e)}
    

async def insert_data_batch1(batch):
    async with semaphore:
        try:
            print("Function is called to update the data")
            conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)  # Increase timeout if necessary
            print("Function is called to update the data1")


            print(batch)
            # Print the SQL data being inserted
            data_list = [
                (data['first_name'], data['phone_number'], data['address'], 
                data['city'], data['state'], data['zip'], data['status'], data['campaign'])
                for data in batch
            ]
            print(f"Data to insert: {data_list}")

            # Execute the batch insert
            await conn.executemany('''
                INSERT INTO maryland_leads(first_name, phone_number, address, city, state, zip, status, campaign) 
                VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ''', data_list)

            print("SQL execution completed")

            # # Batch insert data into the database
            # await conn.executemany('''
            #     INSERT INTO maryland_leads(first_name, phone_number, address, city, state, zip, status) VALUES($1, $2, $3, $4, $5, $6, $7)
            # ''', [(data['first_name'], data['phone_number'], data['address'], data['city'], data['state'], data['zip'], data['status'] ) for data in batch])
            
            print("Function is called to update the data2")
            # Close the connection
            await conn.close()
            print("Function is called to update the data3")
            print("Batch insert successful")
            return {"status": "success"}

        except Exception as e:
            return {"status": "error", "message": str(e)}







async def get_last_interaction(phone_number):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Retrieve the last interaction time from the database
        result = await conn.fetchrow('''
            SELECT last_interaction
            FROM properties
            WHERE phone_number = $1
        ''', phone_number)
        
        await conn.close()

        if result:
            # Convert the result to a dictionary and format the date
            last_interaction = result['last_interaction']
            return {"status": "success", "last_interaction": last_interaction.isoformat() if last_interaction else None}
        else:
            return {"status": "error", "message": "No data found"}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}



@app.route('/api/get_last_interaction/<phone_number>', methods=['GET'])
def get_last_interaction_api(phone_number):
    # Get the last interaction time from the database
    result = asyncio.run(get_last_interaction(phone_number))
    return jsonify(result), 200







async def update_last_interaction(phone_number):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        current_time = datetime.utcnow()  # Use UTC time
        
        # Update last interaction time in the database
        await conn.execute('''
            UPDATE properties
            SET last_interaction = $1
            WHERE phone_number = $2
        ''', current_time, phone_number)
        
        await conn.close()

        return {"status": "success", "message": f"Updated last interaction for record {phone_number}"}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
    


@app.route('/api/update_last_interaction', methods=['POST'])
def update_last_interaction_api():
    phone_number = request.json.get('phone_number')
    if not phone_number:
        return jsonify({"status": "error", "message": "Phone number is required"}), 400

    # Get the existing event loop, or create a new one if it doesn't exist
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the asynchronous task in the current event loop
    result = loop.run_until_complete(update_last_interaction(phone_number))

    return jsonify(result), 200




async def get_data(phone_number):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Retrieve data from the database
        result = await conn.fetch('''
            SELECT *
            FROM properties
            WHERE phone_number = $1
        ''', phone_number)
        
        await conn.close()

        if result:
            return {"status": "success", "data": dict(result[0])}
        else:
            return {"status": "error", "message": "No data found"}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.route('/api/get_data/<phone_number>', methods=['GET'])
def get_data_api(phone_number):
    print("Got the request with the phone number ")
    print(phone_number)
    if not phone_number:
        return jsonify({"status": "error", "message": "Phone number is required"}), 400

    # Get the existing event loop, or create a new one if it doesn't exist
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the asynchronous task in the current event loop
    result = loop.run_until_complete(get_data(phone_number))
    print("Results are ")
    print(result)    
    return jsonify(result), 200













    
async def update_data(phone_number, data):
    try:
        print("We got the request to update the data")
        conn = await asyncpg.connect(DATABASE_URL)
        print("Connected to the database")

        # Update data in the database
        result=await conn.execute('''
            UPDATE properties 
            SET owner_name = $1,
                age_of_ac = $2,
                current_on_payments = $3,
                best_person_to_reach = $4,
                age_of_bathrooms = $5,
                back_taxes = $6,
                amount_behind_on_payment = $7,
                current_on_taxes = $8,
                facing_foreclosure = $9,
                age_of_floors = $10,
                images_consent = $11,
                interest_rate_on_first_mortgage = $12,
                interest_rate_on_second_mortgage = $13,
                age_of_kitchen = $14,
                monthly_afford = $15,
                monthly_mortgage_payment = $16,
                months_behind_on_payment = $17,
                mortgage_balance = $18,
                mortgage_company_name = $19,
                number_of_mortgage_involved = $20,
                other_mortgage = $21,
                person_story = $22,
                address = $23,
                age_of_roof = $24,
                selling_interest = $25,
                structural_damage = $26,
                water_heater_age = $27,
                is_home_owner = $28,
                chat_completed = $29
            WHERE phone_number = $30
        ''', data['owner_name'], data['age_of_ac'], data['current_on_payments'], 
            data['best_person_to_reach'], data['age_of_bathrooms'], data['back_taxes'], 
            data['amount_behind_on_payment'], data['current_on_taxes'], 
            data['facing_foreclosure'], data['age_of_floors'], data['images_consent'], 
            data['interest_rate_on_first_mortgage'], data['interest_rate_on_second_mortgage'], 
            data['age_of_kitchen'], data['monthly_afford'], data['monthly_mortgage_payment'], 
            data['months_behind_on_payment'], data['mortgage_balance'], data['mortgage_company_name'], 
            data['number_of_mortgage_involved'], data['other_mortgage'], data['person_story'], 
            data['address'], data['age_of_roof'], data['selling_interest'], data['structural_damage'], 
            data['water_heater_age'], data['is_home_owner'], data['chat_completed'], 
            phone_number)





        print("The number of effected rows are")
        await conn.close()
        print("The number of effected rows are")
        print(result)        
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}



@app.route('/api/update_data/<phone_number>', methods=['POST'])
def update_data_bulk(phone_number):
    data = request.json
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    print("Making the database request")
    result = loop.run_until_complete(update_data(phone_number, data))
    
    return jsonify(result), 200



@app.route('/api/insert_data', methods=['POST'])
def insert_data_bulk():
    print(f"Raw Data: {request.data}")
    data_list = request.json
    print("Received data")
    
    # Create batches
    batches = [data_list[i:i + BATCH_SIZE] for i in range(0, len(data_list), BATCH_SIZE)]
    total_batches = len(batches)  # Calculate total batches
    print(f"Total batches to process: {total_batches}")

    # Get the existing event loop, or create a new one if it doesn't exist
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the asynchronous tasks in the current event loop
    tasks = [insert_data_batch(batch) for batch in batches]
    results = loop.run_until_complete(asyncio.gather(*tasks))



    # Calculate successful and failed batches
    successful_batches = sum(1 for result in results if result.get("status") == "success")
    failed_batches = total_batches - successful_batches

    # Print the summary
    print(f"Total batches: {total_batches}")
    print(f"Successful batches: {successful_batches}")
    print(f"Failed batches: {failed_batches}")
    
    return jsonify(results), 200









@app.route('/api/roor_webhook', methods=['POST'])
def insert_webhook():
    print(f"Raw Data: {request.data}")
    data = request.json

    # Extract the phone number and remove the leading '1' if it exists
    phone_number = data['from']
    if phone_number.startswith('1'):
        phone_number = phone_number[1:]

    # Call an async function to update the database
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(update_status(phone_number))
    return result

async def update_status(phone_number):
    try:
        print("Function is called with the phone number ", phone_number)
        # Connect to the database
        conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)

        # Check if the phone number exists in the database
        query = "SELECT 1 FROM maryland_leads WHERE phone_number = $1"
        exists = await conn.fetchval(query, phone_number)

        if exists:
            print("The Phone number exists in the database")
            # Update the status to 'replied'
            update_query = "UPDATE maryland_leads SET status = 'replied' WHERE phone_number = $1"
            await conn.execute(update_query, phone_number)
            print(f"Updated status to 'replied' for phone number: {phone_number}")
            response = {"status": "success", "message": "Lead status updated."}
        else:
            print(f"No entry found for phone number: {phone_number}")
            response = {"status": "error", "message": "No entry found for this phone number."}

        # Close the connection
        await conn.close()
        return response

    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}






async def fetch_leads():
    try:
        # Connect to the database
        conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)

        # Query to fetch all leads
        query = """
        SELECT first_name, phone_number, address, city, state, zip, status 
        FROM maryland_leads
        """
        rows = await conn.fetch(query)
        await conn.close()  # Close the connection

        # Convert the result to a DataFrame
        df = pd.DataFrame(rows, columns=['first_name', 'phone_number', 'address', 
                                         'city', 'state', 'zip', 'status'])

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None

@app.route('/api/download_csv', methods=['GET'])
def download_csv():
    # Create an event loop to run the async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    df = loop.run_until_complete(fetch_leads())

    if df is None:
        return jsonify({"error": "Failed to fetch leads"}), 500

    # Save DataFrame to an in-memory buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Return CSV as a file download response
    return Response(
        buffer,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"}
    )




























@app.route('/api/insert_data_from_maryland', methods=['POST'])
def insert_data_from_php():
    print("Function is called")
    """
    Handles data sent from the PHP script and inserts it into the database.
    """
    try:

        data = request.json  # Expecting JSON payload from PHP
        # print(data)
        if not data:
            return jsonify({"status": "error", "message": "No data received"}), 400
        if data:
            data.pop(0)
        
        batches = [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [insert_data_batch1(batch) for batch in batches]
        result = loop.run_until_complete(asyncio.gather(*tasks))
        

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500






if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)  # Changed port to 8000, suitable for reverse proxy setups



# git add .
#git commit -m "Updated API with batch processing and increased timeout"
#git push origin main

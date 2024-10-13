

from flask import Flask, request, jsonify
import asyncio
import asyncpg
from datetime import datetime
app = Flask(__name__)

# Database connection settings
DATABASE_URL = 'postgresql://cocihomesdb_owner:j78CsNBHawmA@ep-damp-water-a5i4eugu.us-east-2.aws.neon.tech/cocihomesdb?sslmode=require'






BATCH_SIZE = 100  # Process 100 rows at a time

async def insert_data_batch(batch):
    try:
        conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)  # Increase timeout if necessary

        # Batch insert data into the database
        await conn.executemany('''
            INSERT INTO properties(owner_name, address, phone_number) VALUES($1, $2, $3)
        ''', [(data['owner_name'], data['address'], data['phone_number']) for data in batch])
        
        # Close the connection
        await conn.close()
        print("Batch insert successful")
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}
    

async def insert_data_batch1(batch):
    try:
        conn = await asyncpg.connect(DATABASE_URL, command_timeout=60)  # Increase timeout if necessary

        # Batch insert data into the database
        await conn.executemany('''
            INSERT INTO maryland_leads(first_name, phone_number, address, city, state, zip, status) VALUES($1, $2, $3, $4, $5, $6, $7)
        ''', [(data['first_name'], data['phone_number'], data['address'], data['city'], data['state'], data['zip'], data['status'] ) for data in batch])
        
        # Close the connection
        await conn.close()
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

    # Get the existing event loop, or create a new one if it doesn't exist
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the asynchronous tasks in the current event loop
    tasks = [insert_data_batch(batch) for batch in batches]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    
    return jsonify(results), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)  # Changed port to 8000, suitable for reverse proxy setups







@app.route('/api/insert_data_from_maryland', methods=['POST'])
def insert_data_from_php():
    """
    Handles data sent from the PHP script and inserts it into the database.
    """
    try:
        data = request.json  # Expecting JSON payload from PHP
        if not data:
            return jsonify({"status": "error", "message": "No data received"}), 400

        # Check if required fields are present
        required_fields = ['first_name', 'phone_number', 'address','city','state','zip','status']
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            return jsonify({"status": "error", "message": f"Missing fields: {', '.join(missing_fields)}"}), 400

        # Create an event loop and insert data asynchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(insert_data_batch1([data]))

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500










# git add .
#git commit -m "Updated API with batch processing and increased timeout"
#git push origin main

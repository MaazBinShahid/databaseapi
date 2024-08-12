

from flask import Flask, request, jsonify
import asyncio
import asyncpg

app = Flask(__name__)

# Database connection settings
DATABASE_URL = 'postgresql://cocihomesdb_owner:j78CsNBHawmA@ep-damp-water-a5i4eugu.us-east-2.aws.neon.tech/cocihomesdb?sslmode=require'

async def insert_data(data):
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Insert data into the database
        await conn.execute('''
            INSERT INTO properties(owner_name, address, phone_number) VALUES($1, $2, $3)
        ''', data['owner_name'], data['address'], data['phone_number'])


        
        # Close the connection
        await conn.close()
        print("Success")
        return {"status": "success"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.route('/api/insert_data', methods=['POST'])
def insert_data_bulk():
    print(f"Raw Data: {request.data}")
    data_list = request.json
    print("Got the data")
    print(data_list)

    # Get the existing event loop, or create a new one if it doesn't exist
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the asynchronous tasks in the current event loop
    tasks = [insert_data(data) for data in data_list]
    results = loop.run_until_complete(asyncio.gather(*tasks))
    
    return jsonify(results), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)  # Changed port to 8000, suitable for reverse proxy setups


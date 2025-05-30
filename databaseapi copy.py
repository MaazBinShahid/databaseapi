

from flask import Flask, request, jsonify, Response, send_file
import asyncio
import asyncpg
from datetime import datetime

import pandas as pd
import io
app = Flask(__name__)

# Database connection settings
DATABASE_URL = 'postgresql://cocihomesdb_owner:j78CsNBHawmA@ep-damp-water-a5i4eugu.us-east-2.aws.neon.tech/cocihomesdb?sslmode=require'

DATABASE_URL1 = 'postgresql://tolldata_owner:npg_3h8STbuBJCvG@ep-ancient-glitter-a59nplz4-pooler.us-east-2.aws.neon.tech/tolldata?sslmode=require'
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
    


# async def insert_data_batch_toll(batch):
#     async with semaphore:
#         try:
#             conn = await asyncpg.connect(DATABASE_URL1, command_timeout=60)

#             data_list = [
#                 (data['Date_Posted'], data['Transaction'], data['Receipt'], 
#                 data['TransponderPlate'], data['Agency'], data['EntryPlaza'], data['ExitPlaza'], data['EntryDate_and_Time'],
#                 data['ExitDateandTime'], data['PlazaFacility'], data['Amount'], data['Balance'])       
#                 for data in batch
#             ]
#             print(f"Data to insert: {data_list[0]}")



#             await conn.executemany('''
#                 INSERT INTO toll_transactions(
#                     date_posted,
#                     transaction,
#                     receipt,
#                     transponder_plate,
#                     agency,
#                     entry_plaza,
#                     exit_plaza,
#                     entry_datetime,
#                     exit_datetime,
#                     plaza_facility,
#                     amount,
#                     balance
#                 ) VALUES (
#                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
#                 )
#             ''', data_list)

#             await conn.close()
#             print(f"Batch insert successful: {len(batch)} toll transactions uploaded.")
#             return {"status": "success"}

#         except Exception as e:
#             return {"status": "error", "message": str(e)}







@app.route('/api/get_toll_data', methods=['GET'])
def get_toll_data():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        df = loop.run_until_complete(fetch_toll_data())
        if df is None or df.empty:
            return jsonify([]), 200

        print(df)
        # Convert datetime columns to ISO strings or None
        datetime_cols = ['date_posted', 'entry_datetime', 'exit_datetime']
        for col in datetime_cols:
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        # Replace any other NaN or NaT with None (for JSON serialization)
        df = df.where(pd.notnull(df), None)



        return jsonify(df.to_dict(orient="records")), 200

    except Exception as e:
        print("Error in /api/get_toll_data:", e)
        return jsonify({"status": "error", "message": str(e)}), 500


async def fetch_toll_data():
    try:
        print("Get Toll Data")
        conn = await asyncpg.connect(DATABASE_URL1, command_timeout=60)

        query = """
        SELECT date_posted, transaction, receipt, transponder_plate, agency,
               entry_plaza, exit_plaza, entry_datetime, exit_datetime,
               plaza_facility, amount, balance
        FROM toll_transactions
        ORDER BY date_posted DESC
        LIMIT 1000
        """
        rows = await conn.fetch(query)
        await conn.close()

        df = pd.DataFrame(rows, columns=[
            "date_posted", "transaction", "receipt", "transponder_plate",
            "agency", "entry_plaza", "exit_plaza", "entry_datetime",
            "exit_datetime", "plaza_facility", "amount", "balance"
        ])
        return df

    except Exception as e:
        print(f"Error fetching toll data: {e}")
        return None



def parse_datetime(val):
    if not val:
        return None
    try:
        return datetime.strptime(val, "%m/%d/%Y %H:%M")
    except:
        try:
            return datetime.strptime(val, "%m/%d/%Y")
        except:
            return None

def parse_float(val):
    try:
        return float(val)
    except:
        return None


async def insert_data_batch_toll(batch):
    async with semaphore:
        try:
            conn = await asyncpg.connect(DATABASE_URL1, command_timeout=60)

            data_list = [
                (
                    parse_datetime(data['Date_Posted']),
                    data['Transaction'],
                    data['Receipt'],
                    data['TransponderPlate'],
                    data['Agency'],
                    data['EntryPlaza'],
                    data['ExitPlaza'],
                    parse_datetime(data['EntryDate_and_Time']),
                    parse_datetime(data['ExitDateandTime']),
                    data['PlazaFacility'],
                    parse_float(data['Amount']),
                    parse_float(data['Balance'])
                )
                for data in batch
            ]

            insert_query = '''
                INSERT INTO toll_transactions(
                    date_posted,
                    transaction,
                    receipt,
                    transponder_plate,
                    agency,
                    entry_plaza,
                    exit_plaza,
                    entry_datetime,
                    exit_datetime,
                    plaza_facility,
                    amount,
                    balance
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                )
                ON CONFLICT (date_posted, balance) 
                DO UPDATE SET balance = toll_transactions.balance  -- no real change
                RETURNING id, CASE WHEN xmax = 0 THEN 'inserted' ELSE 'skipped' END AS status;
            '''
            skipped_count = 0
            inserted_count = 0
            async with conn.transaction():
                for row in data_list:
                    try:
                        result = await conn.fetchrow(insert_query, *row)
                        if result['status'] == 'inserted':
                            inserted_count += 1
                        else:
                            skipped_count += 1
                    except Exception as e:
                        print(f"Skipping row due to error: {e}")
                        skipped_count += 1
            await conn.close()
            print(f"411Batch insert successful: {inserted_count}  new toll transactions uploaded and {skipped_count} are skipped.")
            return {"status": "success", "inserted_rows": inserted_count, "11skipped_rows" : skipped_count}

        except Exception as e:
            print("Error occurred during batch insert:", e)
            return {"status": "error", "message": str(e)}




@app.route('/api/keeneyesData', methods=['POST'])
def insert_data_from_php1():
    print("Function is called")
    try:
       
        data = request.json  # Expecting JSON payload from PHP
        print(data)
        if not data:
            return jsonify({"status": "error", "message": "No data received"}), 400

        # Remove headers if needed (as before)
        if isinstance(data[0], dict) and "Date_Posted" not in data[0]:
            data.pop(0)


        print(f"Total rows received: {len(data)}")
        batches = [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]
        print(f"Total batches created: {len(batches)}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [insert_data_batch_toll(batch) for batch in batches]
        result = loop.run_until_complete(asyncio.gather(*tasks))
        print(result)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500






if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)  # Changed port to 8000, suitable for reverse proxy setups



# git add .
#git commit -m "Updated API with batch processing and increased timeout"
#git push origin main

#python databaseapi.py
#ngrok http 8000
<?php
session_start(); // Start a session to store the $data array between requests
require_once 'include/db.php';
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
?>

<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Upload Data</title>
    <link rel="icon" type="image/png" sizes="16x16" href="../assets/images/favicon.png">
    <link href="../assets/vendor/bootstrap-select/dist/css/bootstrap-select.min.css" rel="stylesheet">
    <link href="../assets/css/style.css" rel="stylesheet">
</head>
<body>

<div id="main-wrapper">
    <?php include 'include/nav.php'; ?>
    <?php include 'include/sidebar.php'; ?>

    <div class="content-body">
        <div class="container-fluid">
            <div class="page-titles">
                <ol class="breadcrumb">
                    <li class="breadcrumb-item"><a href="javascript:void(0)">Admin Panel</a></li>
                    <li class="breadcrumb-item active"><a href="javascript:void(0)">Upload Data</a></li>
                </ol>
            </div>
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h4 class="card-title">Fetch Toll Transaction Data from API</h4>
                        </div>
                        <div class="card-body">
                            <!-- Button to Trigger API Call -->
                            <form method="POST">
                                <button type="submit" name="fetch_api_data" class="btn btn-primary">Fetch Data from API</button>
                            </form>

                            <?php
                            if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['fetch_api_data'])) {
                                $apiUrl = 'https://8b78-223-123-7-17.ngrok-free.app/api/get_toll_data';

                                $ch = curl_init($apiUrl);
                                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                                curl_setopt($ch, CURLOPT_HTTPGET, true);
                                curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);

                                $response = curl_exec($ch);
                                $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
                                curl_close($ch);

                                if ($httpCode === 200 && $response) {
                                    $data = json_decode($response, true);

                                    if (!empty($data)) {
                                        echo "<div class='table-responsive mt-4'>";
                                        echo "<table class='table table-bordered'>";
                                        echo "<thead><tr>
                                            <th>Date Posted</th>
                                            <th>Transaction</th>
                                            <th>Receipt</th>
                                            <th>Transponder Plate</th>
                                            <th>Agency</th>
                                            <th>Entry Plaza</th>
                                            <th>Exit Plaza</th>
                                            <th>Entry Datetime</th>
                                            <th>Exit Datetime</th>
                                            <th>Plaza Facility</th>
                                            <th>Amount</th>
                                            <th>Balance</th>
                                        </tr></thead><tbody>";

                                        foreach ($data as $row) {
                                            echo "<tr>
                                                <td>{$row['date_posted']}</td>
                                                <td>{$row['transaction']}</td>
                                                <td>{$row['receipt']}</td>
                                                <td>{$row['transponder_plate']}</td>
                                                <td>{$row['agency']}</td>
                                                <td>{$row['entry_plaza']}</td>
                                                <td>{$row['exit_plaza']}</td>
                                                <td>{$row['entry_datetime']}</td>
                                                <td>{$row['exit_datetime']}</td>
                                                <td>{$row['plaza_facility']}</td>
                                                <td>{$row['amount']}</td>
                                                <td>{$row['balance']}</td>
                                            </tr>";
                                        }

                                        echo "</tbody></table></div>";
                                    } else {
                                        echo "<div class='alert alert-warning mt-3'>No data returned from API.</div>";
                                    }
                                } else {
                                    echo "<div class='alert alert-danger mt-3'>Failed to fetch data. HTTP Code: $httpCode</div>";
                                }
                            }
                            ?>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <?php include 'include/footer.php'; ?>
</div>

<script src="../assets/vendor/global/global.min.js"></script>
<script src="../assets/vendor/bootstrap-select/dist/js/bootstrap-select.min.js"></script>
<script src="../assets/js/custom.min.js"></script>
<script src="../assets/js/deznav-init.js"></script>

</body>
</html>

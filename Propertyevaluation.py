import requests

def get_property_value(address, property_type, bedrooms, bathrooms, square_footage, api_key, comp_count=5):
    # Construct the URL with the given parameters
    url = f"https://api.rentcast.io/v1/avm/value?address={address}&propertyType={property_type}&bedrooms={bedrooms}&bathrooms={bathrooms}&squareFootage={square_footage}&compCount={comp_count}"
    
    # Set the headers with the API key for authentication
    headers = {
        "accept": "application/json",
        "X-Api-Key": f"{api_key}"  # Add API key in the authorization header if required
    }
    
    # Make the GET request to the API
    response = requests.get(url, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Return the JSON response
        return response.json()
    else:
        # Handle the error case
        return {"error": f"Failed to retrieve data. Status code: {response.status_code}"}

# Example usage
api_key = "d4c21d49832f4cf8874cb5f193398482"
address = "5500 Grand Lake Drive, San Antonio, TX, 78244"
property_type = "Single Family"
bedrooms = 4
bathrooms = 2
square_footage = 1600

property_data = get_property_value(address, property_type, bedrooms, bathrooms, square_footage, api_key)

# Display the property data or handle it as needed
print(property_data)

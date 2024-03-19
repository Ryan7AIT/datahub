from geopy.geocoders import Nominatim

def get_street_address(latitude, longitude):
    geolocator = Nominatim(user_agent="my_app")
    location = geolocator.reverse((latitude, longitude))
    address = location.raw['address']

    street = address.get('road', '')
    city = address.get('city', '')
    state = address.get('state', '')
    return address
# Example usage
latitude = 36.685685
longitude = 3.354008
street = get_street_address(latitude, longitude)
# print(f"Street: {street}")
# print(f"City: {city}")
# print(f"State: {state}")

print(street)


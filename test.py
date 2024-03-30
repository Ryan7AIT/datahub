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
# street = get_street_address(latitude, longitude)
# # print(f"Street: {street}")
# # print(f"City: {city}")
# # print(f"State: {state}")

# print(street)


from datetime import datetime

# Parse the timestamps
old_time = datetime.strptime('2024-03-30 02:48:47.486479', '%Y-%m-%d %H:%M:%S.%f')
new_time = datetime.strptime('2024-03-30 02:48:49.486479', '%Y-%m-%d %H:%M:%S.%f')

# Calculate the difference in hours
# time_diff_hours = (new_time - old_time).total_seconds() / 3600

# print(time_diff_hours)

from math import radians, sin, cos, sqrt, atan2

def calculate_distance(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6371.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

# Test the function
lat1 = 52.2296756
lon1 = 21.0122287
lat2 = 52.406374
lon2 = 16.9251681

print(calculate_distance(lat1, lon1, lat2, lon2))



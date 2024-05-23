from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut


def get_street_address(p):
    latitude, longitude = p.split(',')
    geolocator = Nominatim(user_agent="my_app", timeout=10)  

    try:
        location = geolocator.reverse((latitude, longitude))
        address = location.raw['address']
        return address['state'] , address['town']
    except GeocoderTimedOut:
        return get_street_address(p)
    except Exception as e:
        return ["", "", ""]
    

print(get_street_address("13.4,33.4")) # ['Tanger', 'Tanger-Assilah', 'المغرب']
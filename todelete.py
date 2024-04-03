from datetime import datetime
from datetime import datetime
from datetime import datetime
def calculate_hour_difference(start_date, end_date):
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    hour_difference = (end_datetime - start_datetime).total_seconds() /60 
    return hour_difference

start_date = "2024-02-28 02:12:45"
end_date = "2024-02-28 02:30:34"
difference_in_hours = calculate_hour_difference(start_date, end_date)
print("The difference in hours is:", difference_in_hours)
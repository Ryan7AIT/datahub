import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

# Assume we have a DataFrame `df` with columns 'Timestamp', 'Car_ID', 'Latitude', 'Longitude', 'Speed'
# For example:
df = pd.read_csv('/Users/mac/Downloads/trainingdataset.csv')

# For simplicity, let's predict the speed at the next time step based on the current speed, latitude, and longitude
df['Next_Speed'] = df['speed'].shift(-1)

# Drop the last row, which has a NaN 'Next_Speed'
df = df.iloc[:-1]

print(df.head())

# Define the input variables (features) and the output variable (target)
# features = ['speed', 'latitude', 'longitude']
features = ['speed']

target = 'Next_Speed'

# Split the data into a training set and a test set
X_train, X_test, y_train, y_test = train_test_split(df[features], df[target], test_size=0.2, random_state=42)

# Create and train the model
model = LinearRegression()
model.fit(X_train, y_train)



# Evaluate the model
train_score = model.score(X_train, y_train)
test_score = model.score(X_test, y_test)

print(f'Train score: {train_score}')
print(f'Test score: {test_score}')


# Use the model to make predictions on the test set
y_pred = model.predict(X_test)

def predict_next_speed(speed, latitude, longitude):
    # Create a DataFrame containing the input values
    input_data = pd.DataFrame({
        'speed': [speed]
        # 'latitude': [latitude],
        # 'longitude': [longitude]
    })

    # Use the model to make a prediction
    prediction = model.predict(input_data)

    # Return the predicted next speed
    return prediction[0]

next_speed = predict_next_speed(50, 40.7128, -74.0060)
print('Predicted next speed:', next_speed)

# Plot the actual vs. predicted speeds
plt.scatter(y_test, y_pred)
plt.xlabel('Actual Speed')
plt.ylabel('Predicted Speed')
plt.title('Actual vs. Predicted Speeds')
plt.show()

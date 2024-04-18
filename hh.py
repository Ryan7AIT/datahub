import pickle
import numpy as np


# Load the model from the file
with open('speeding_model.pkl', 'rb') as file:
    model = pickle.load(file)

# Now you can use the model to make predictions
new_speed = np.array([[75]])
prediction = model.predict(new_speed)

if prediction == 1:
    print("The car is speeding.")
else:
    print("The car is not speeding.")
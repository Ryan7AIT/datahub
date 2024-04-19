import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix

# Generate 1000 random speeds between 30 and 120
np.random.seed(0)
speeds = np.random.uniform(30, 120, 1000).reshape(-1, 1)

# Create labels: 1 if the speed is above 70, 0 otherwise
labels = (speeds > 70).astype(int)

# Split the data into a training set and a test set
X_train, X_test, y_train, y_test = train_test_split(speeds, labels, test_size=0.2, random_state=0)

# Create and train the logistic regression model
model = LogisticRegression()
model.fit(X_train, y_train.ravel())


import pickle

# # Save the trained model to a file
# with open('speeding_model.pkl', 'wb') as file:
#     pickle.dump(model, file)


# Later, load the model
from keras.models import load_model
model = load_model('pm_model.h5')
  
# Test the model
predictions = model.predict(X_test)

# Calculate accuracy, precision and recall
accuracy = accuracy_score(y_test, predictions)
precision = precision_score(y_test, predictions)
recall = recall_score(y_test, predictions)

print(f"Accuracy: {accuracy}")
print(f"Precision: {precision}")
print(f"Recall: {recall}")

# Calculate and print the confusion matrix
confusion = confusion_matrix(y_test, predictions)
print(f"Confusion matrix:\n{confusion}")

# Predict whether a car going at 75 km/h is speeding
new_speed = np.array([[75]])
prediction = model.predict(new_speed)

if prediction == 1:
    print("The car is speeding.")
else:
    print("The car is not speeding.")
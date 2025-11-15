import json
import pandas as pd

# Read the JSON file
with open('aeroplane_model.json', 'r') as f:
    data = json.load(f)

# Flatten the nested structure
rows = []
for manufacturer, models in data.items():
    for model, specs in models.items():
        row = {
            'manufacturer': manufacturer,
            'model': model,
            'max_seats': specs['max_seats'],
            'max_weight': specs['max_weight'],
            'max_distance': specs['max_distance'],
            'engine_type': specs['engine_type']
        }
        rows.append(row)

# Create DataFrame
df = pd.DataFrame(rows)

# Save to CSV
df.to_csv('aeroplane_model.csv', index=False)

print("CSV file created successfully!")
print(df)
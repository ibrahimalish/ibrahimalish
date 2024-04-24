import pandas as pd
import re

# Define the number of rows to load
num_rows = 100

# Load the first 10 rows of the dataset
data = pd.read_json("sample.json", lines=True, nrows=num_rows)

# Function to extract text from HTML
def extract_text(html_list):
    clean = re.compile('<.*?>')
    return [re.sub(clean, '', html) for html in html_list]

# Clean and preprocess the data
data['description'] = data['description'].apply(lambda x: extract_text(x))

# Extract minimum price from the range if present
def extract_price(price):
    if isinstance(price, str) and price.strip():  # Check if the price string is not empty
        price = price.strip()
        if '-' in price:
            price_range = [float(p.strip().strip('$')) for p in price.split('-')]
            return min(price_range)
        else:
            return float(price.strip('$').strip())
    else:
        return None  # Return None if the price string is empty or not a string


# Apply price extraction to the 'price' column
data['price'] = data['price'].apply(extract_price)

# Extract 'title' and 'asin' columns
data = data[['price', 'title', 'asin']]

# Save preprocessed data as JSON
file_location = "preprocessed_data.json"
data.to_json(file_location, orient="records")

print("Preprocessed data saved as '{}'.".format(file_location))

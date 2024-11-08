import re
import pandas as pd
import usaddress

def get_address(addr) -> tuple:
    try:
        if addr:
            # Use usaddress to tag the address and return as a tuple
            data = usaddress.tag(addr)[0]
            return (
                data.get('AddressNumber', None),
                data.get('StreetName', None),
                data.get('PlaceName', None),
                data.get('StateName', None),
                data.get('ZipCode', None),
                data.get('CountryName', None)
            )
        return (None, None, None, None, None, None)
    except Exception as e:
        # Return a tuple of None values if parsing fails
        return (None, None, None, None, None, None)

def standardize_name(name):
    # Convert to title case
    name = name.strip().title()
    # Remove special characters
    name = re.sub(r'[^\w\s]', '', name)
    # Replace multiple spaces with a single space
    name = re.sub(r'\s+', ' ', name)
    return name


# Helper function to split full name into first, middle, and last names
def split_name(name):
    try:
        name = standardize_name(name)
        parts = name.split()
        if len(parts) == 1:
            return parts[0], '', ''  
        elif len(parts) == 2:
            return parts[0], '', parts[1]  
        else:
            return parts[0], ' '.join(parts[1:-1]), parts[-1]
    except:
        return None,None,None

def clean_string(value):
    if isinstance(value, str):
        # Remove special characters and extra spaces
        value = re.sub(r'[^A-Za-z0-9\s]', '', value)  # Remove non-alphanumeric characters
        value = ' '.join(value.split())  # Remove extra spaces
        return value
    return value

def clean_dataframe(df: pd.DataFrame, columns_to_standardize: dict = None):
     # Standardize values in specific columns if needed
    if columns_to_standardize:
        for col, func in columns_to_standardize.items():
            if col in df.columns:
                df[col] = df[col].apply(func)

    # Remove special characters and extra spaces from column values
    df = df.map(clean_string)
    df = df.drop_duplicates()
    return df

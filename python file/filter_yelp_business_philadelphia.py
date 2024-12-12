import json

# Input and output file paths
input_file = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_business.json'
output_file = '/Users/yinxc/Desktop/CS689/termprojectdata/yelp_business_philadelphia.json'

# Filter and write data to a new file
with open(input_file, 'r') as inputfile, open(output_file, 'w') as outfile:
    for line in inputfile:
        record = json.loads(line)
        # Check if the city is Philadelphia
        if record.get("city") == "Philadelphia":
            json.dump(record, outfile)
            outfile.write('\n')

print(f"Filtered data has been saved.")

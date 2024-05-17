import json
import xmltodict
import os 
import logging
    
def xml_to_json(
    xml_path: str,
    output_path: str = None
):
    """
    Transform an XML file into a JSON format.
    """    
    
    # Open the input XML file and read data in form of python dictionary using xmltodict module.
    try:
        with open(xml_path) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
    except FileNotFoundError as error:
        logging.error(f"Error: {str(error)} (XML file not found)")
    except Exception as error:
        logging.error(f"Error: {str(error)}")
      
    # Generate the object using json.dumps() corresponding to JSON data.
    json_data = json.dumps(data_dict)

    # Check if output_path is provided, otherwise set it to a default value (XML folder path).
    if output_path is None:
        output_path = os.path.dirname(xml_path)
        output_directory = f'{output_path}/json'
    else:
        output_directory = f'{output_path}/json'

    # Set the name of the json file to the name of the XML file provided.
    file_name = os.path.splitext(os.path.basename(xml_path))[0]
        
    # Create the output directory if it doesn't exist.
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Write the contents of the JSON file into the folder path with a progress bar.
    file_path = os.path.join(output_directory, f'{file_name}.json')
    try:
        with open(file_path, "w") as json_file:
            json_file.write(json_data)
    except Exception as error:
        logging.error(f"Error: {str(error)}")
        
    return json_data
import requests
import zipfile
import io

class zipFileExtractor:
    def __init__(self,input_url,output_directory):
        self.input_url = input_url
        self.output_directory = output_directory
    
    def download_file(self,file_name):
        response = requests.get(self.input_url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zipf:
                if file_name in zipf.namelist():
                    with zipf.open(file_name) as file:
                        with open(self.output_directory,'wb') as f:
                            f.write(file.read())
                else:
                    raise Exception(f"Provided filename '{file_name}' not found")
            return self.output_directory
        else:
            raise Exception(f"Download failed - HTTPError:'{response.status_code}'")
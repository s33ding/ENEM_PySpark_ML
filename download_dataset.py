import io
import os
import requests

def download_file_from_drive(destination_path, link):
    # Extract the file ID from the shared link
    file_id = link.split('/')[-2]

    # Request the file from Google Drive
    response = requests.get(f"https://drive.google.com/uc?id={file_id}", stream=True)

    # Save the file to the destination path
    with open(destination_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    print(f"File downloaded successfully: {destination_path}")

# Example usage:
file_dict = {
    'dataset/enem.csv': 'https://drive.google.com/file/d/1vifl54iaaZixl9LDr6vrqjhWZx6f8sVg/view?usp=sharing',
    'dataset/score_cn.csv': 'https://drive.google.com/file/d/1bEcKZQIArjVbv4WNtBUQ6wLlEa52OkXH/view?usp=drive_link',
    'dataset/score_cr.csv': 'https://drive.google.com/file/d/1AoL7d5A07rAj639ZfYBCMbFkH859P09X/view?usp=drive_link'
}

for file_name, link in file_dict.items():
    download_file_from_drive(file_name, link)

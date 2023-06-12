import gdown

# Example usage:
file_dict = {
    'dataset/enem.csv': 'https://drive.google.com/file/d/1U1RxZsWKMtp1Rv-VBZ0uBAs1l_kXPX4F/view?usp=drive_link',
    'dataset/score_cn.csv': 'https://drive.google.com/file/d/1bEcKZQIArjVbv4WNtBUQ6wLlEa52OkXH/view?usp=drive_link',
    'dataset/score_cr.csv': 'https://drive.google.com/file/d/1AoL7d5A07rAj639ZfYBCMbFkH859P09X/view?usp=drive_link'
}

for file_name, link in file_dict.items():
    # Extract the file ID from the shared link
    file_id = link.split('/')[-2]

    # Construct the download URL using the file ID
    download_url = f"https://drive.google.com/uc?id={file_id}"

    # Download the file using gdown
    gdown.download(download_url, file_name, quiet=False)

    print(f"File downloaded successfully: {file_name}")

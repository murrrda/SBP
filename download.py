import kagglehub

# Download latest version
path = kagglehub.dataset_download("danushkumarv/netherlands-housing-analytics", output_dir='./data')

print("Path to dataset files:", path)

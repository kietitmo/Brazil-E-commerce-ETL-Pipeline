import pandas as pd

df = pd.read_csv('./data/csv_files/olist_products_dataset.csv', sep=',', header='infer', names=None)
# Loại bỏ các dòng chứa giá trị null hoặc NaN
df_cleaned = df.dropna()

# In ra kết quả sau khi loại bỏ các dòng chứa null hoặc NaN
print(df_cleaned)

# Nếu muốn lưu lại file đã được xử lý
df_cleaned.to_csv('./data/csv_files/olist_products_dataset.csv', index=False)

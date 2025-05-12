import pandas as pd

# 处理消息表为数据表
def process_file(input_file, output_file):
    # 读取文件
    data = pd.read_csv(input_file, sep="|", header=None, names=["c_custkey",	"c_name",	"revenue",	"c_acctbal"	,"n_name",	"c_address",	"c_phone",	"c_comment"])
    data = data.dropna(subset=["c_custkey"])
    data["c_custkey"] = data["c_custkey"].astype(int)
    # 按第一列去重，保留最后出现的行
    data = data.drop_duplicates(subset="c_custkey", keep="last")
    
    # 按第三列降序排序
    data = data.sort_values(by="revenue", ascending=False)

    data["revenue"] = data["revenue"].round(4)

    
    # 找到实际内容不同的行 ID
    unique_ids = data["c_custkey"].tolist()
    
    # 保存结果到输出文件
    data.to_csv(output_file, sep="|", index=False, header=False)
    
    # 打印实际内容不同的行 ID
    print("Unique IDs:", len(unique_ids))

# 示例调用
input_file = "../../../../../cquirrel-DataGenerator/q10result.txt"  # 输入文件路径
output_file = "../../../../../cquirrel-DataGenerator/q10result-table.txt"  # 输出文件路径
process_file(input_file, output_file)


# 对比数据表
# 读取两个文件
table_file = "../../../../../cquirrel-DataGenerator/q10result-table.txt"
real_file = "../../../../../cquirrel-DataGenerator/q10-real.txt"

# 定义列名
columns = ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"]

# 加载数据
table_data = pd.read_csv(table_file, sep="|", header=None, names=columns)
real_data = pd.read_csv(real_file, sep="|", header=None, names=columns)

# 提取 c_custkey 列
table_keys = set(table_data["c_custkey"])
real_keys = set(real_data["c_custkey"])

# 找出差异
only_in_table = table_keys - real_keys
only_in_real = real_keys - table_keys

# 打印结果
print("Only in q10result-table.txt:", sorted(only_in_table))
print("Only in q10-real.txt:", sorted(only_in_real))
print(len(only_in_real), "keys only in q10-real.txt")

# 找出 c_custkey 相同但 revenue 差异大于 0.1 的数据
merged_data = pd.merge(table_data, real_data, on="c_custkey", suffixes=("_table", "_real"))
revenue_diff = merged_data[abs(merged_data["revenue_table"] - merged_data["revenue_real"]) > 0.1]

# 打印结果
print("c_custkey 相同但 revenue 差异大于 0.1 的数据:")
print(revenue_diff[["c_custkey", "revenue_table", "revenue_real"]])
print(revenue_diff["c_custkey"].tolist())
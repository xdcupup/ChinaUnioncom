import os

import pandas as pd




def group_source(flie):
    excel_file = f'C:/Users/14659/Desktop/集体/{flie}'  # 替换为你的Excel文件路径
    sheet_name = 1  # 第二个工作表的索引为1

    # 使用pandas的read_excel函数读取Excel文件并指定sheet_name参数为要读取的工作表的索引或名称
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    # 打印第二个工作表的数据
    print(df)
    output_excel_file = '集体.xlsx'  # 导出的Excel文件路径
    df.to_excel(output_excel_file, index=False)


import pandas as pd

# 集体
def group_source02():
    global file



    # 指定要遍历的文件夹路径
    folder_path = 'C:/Users/14659/Desktop/集体5月'

    # 初始化一个空数组用于存储文件路径
    file_paths = []

    # 使用 os.listdir() 方法获取文件夹中的所有文件名
    files = os.listdir(folder_path)

    # 遍历文件名并获取文件的完整路径并添加到数组中
    for file in files:
        file_path = os.path.join(folder_path, file)
        file_paths.append(file_path)

    # 打印数组中的文件路径
    print(file_paths)

    # 初始化一个空的 DataFrame，用于存储所有第二个工作表的内容
    merged_df = pd.DataFrame()

    # 遍历每个 Excel 文件，读取第二个工作表，并将其内容添加到 merged_df 中
    for file in file_paths:
        # 读取每个 Excel 文件的第二个工作表
        df = pd.read_excel(file, sheet_name='服务窗口集体成员')  # 第二个工作表的索引为1

        # 将第二个工作表的内容添加到 merged_df 中
        merged_df = merged_df.append(df, ignore_index=True)

    # 将合并后的 DataFrame 写入到一个新的 Excel 文件中的一个工作表中
    output_excel_file = 'merged_output.xlsx'  # 合并后的 Excel 文件路径
    merged_df.to_excel(output_excel_file, index=False)

    print(f"多个 Excel 文件的第二个工作表已成功合并到 {output_excel_file} 文件中的一个工作表中。")



# 个人
def personal_path():
    global files, file
    # 指定要遍历的文件夹路径
    folder_path = 'C:\\Users\\14659\\Desktop\\新建文件夹'
    # 初始化一个空数组用于存储文件路径
    file_paths = []
    # 使用 os.listdir() 方法获取文件夹中的所有文件名
    files = os.listdir(folder_path)
    # 遍历文件名并获取文件的完整路径并添加到数组中
    for file in files:
        file_path = os.path.join(folder_path, file)
        file_paths.append(file_path)
    # 打印数组中的文件路径
    print(file_paths)
    # 初始化一个空的 DataFrame，用于存储所有第二个工作表的内容
    merged_df = pd.DataFrame()
    # 遍历每个 Excel 文件，读取第二个工作表，并将其内容添加到 merged_df 中
    for file in file_paths:
        # 读取每个 Excel 文件的第二个工作表
        df = pd.read_excel(file, sheet_name='服务窗口个人')

        # 将第二个工作表的内容添加到 merged_df 中
        merged_df = merged_df.append(df, ignore_index=True)
    # 将合并后的 DataFrame 写入到一个新的 Excel 文件中的一个工作表中
    output_excel_file = '服务窗口个人.xlsx'  # 合并后的 Excel 文件路径
    merged_df.to_excel(output_excel_file, index=False)
    print(f"多个 Excel 文件的第二个工作表已成功合并到 {output_excel_file} 文件中的一个工作表中。")


# 政企
def personal_path():
    global files, file
    # 指定要遍历的文件夹路径
    folder_path = 'C:\\Users\\14659\\Desktop\\新建文件夹'
    # 初始化一个空数组用于存储文件路径
    file_paths = []
    # 使用 os.listdir() 方法获取文件夹中的所有文件名
    files = os.listdir(folder_path)
    # 遍历文件名并获取文件的完整路径并添加到数组中
    for file in files:
        file_path = os.path.join(folder_path, file)
        file_paths.append(file_path)
    # 打印数组中的文件路径
    print(file_paths)
    # 初始化一个空的 DataFrame，用于存储所有第二个工作表的内容
    merged_df = pd.DataFrame()
    # 遍历每个 Excel 文件，读取第二个工作表，并将其内容添加到 merged_df 中
    for file in file_paths:
        # 读取每个 Excel 文件的第二个工作表
        df = pd.read_excel(file, sheet_name='服务窗口个人')

        # 将第二个工作表的内容添加到 merged_df 中
        merged_df = merged_df.append(df, ignore_index=True)
    # 将合并后的 DataFrame 写入到一个新的 Excel 文件中的一个工作表中
    output_excel_file = '服务窗口个人.xlsx'  # 合并后的 Excel 文件路径
    merged_df.to_excel(output_excel_file, index=False)
    print(f"多个 Excel 文件的第二个工作表已成功合并到 {output_excel_file} 文件中的一个工作表中。")





# personal_path()
group_source02()

# 输入要扫描的文件夹路径
scan_folder_path = r"C:\biliup\backup"


# 定义要执行的CMD命令
cmd_command = 'telegram-upload --caption "%s      #%s #%s" --to https://t.me/LuBozhan "%s"'

# 上传超时次数
up_out_count = 3

# 存放日志文件的文件夹路径（请替换为你的目录）
log_folder = ".//logs"

# 下载位置
download_index = -1


# 休息
sleep_time = 60 * 30
# sleep_time = 1
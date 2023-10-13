# 导入系统操作模块
import glob
import os
import random

import pandas as pd
import time
# 导入时间模块
import traceback

import sys
import subprocess
from datetime import datetime, timedelta
# 导入后台调度模块
from apscheduler.schedulers.blocking import BlockingScheduler
import Base
import Config
import Utils






# @Utils.execution_time_decorator(60 * 29)
def main() -> None:

    Utils.writer_csv("new")

    # 检查文件夹是否存在
    if not os.path.exists(Config.scan_folder_path):

        print("指定的文件夹路径不存在。")

    else:

        # 获取文件夹中的文件列表
        files = [file for file in glob.glob(Config.scan_folder_path + "/*") if os.path.isfile(file)]



        files_length = len(files)



        # 判断文件夹中文件没用的话，程序将会暂停 20 分钟
        if files_length == 0:
            # 上传超时次数可以在此处置空
            Config.up_out_count = 0

            Utils.log_current_thread_and_datetime("INFO",
                                                  f" 路径: {Config.scan_folder_path} , 文件数量暂时为 0 , 程序将会休眠{Config.sleep_time}秒",
                                                  Config.log_folder)
            time.sleep(Config.sleep_time)
            return

        # 根据文件创建时间进行排序
        sorted_files = sorted(files, key=os.path.getmtime)

        # 取出修改时间最早修改的文件地址
        file_path = sorted_files[Config.download_index]

        # 文件名 和 扩展名
        file_name, file_extension = os.path.splitext(file_path)

        # 基础文件名
        file_base_name = os.path.basename(file_path)



        # 文件最后修改时间
        last_modified_time = os.path.getmtime(file_path)

        # 转换为 datetime 对象
        last_modified_datetime = datetime.fromtimestamp(last_modified_time)

        # 格式化为特定格式字符串
        formatted_time = last_modified_datetime.strftime(Base.date_formatter_common)

        # 获取文件大小（字节数）
        file_size_bytes = os.path.getsize(file_path)

        # 转换为兆字节
        file_size_mb = file_size_bytes / (1024 * 1024)  # 1 MB = 1024 * 1024 bytes


        # 如果该文件信息已经存在
        find_index = Utils.find_df("FILE_NAME", file_base_name)

        if find_index:

            s1 = Utils.find_index(find_index)

            if s1 is not None:

                up_count = s1["UP_COUNT"]
                is_delete = s1["IS_DELETE"]
                status = s1["STATUS"]

                # 上传次数超过 3 次将会被限制上传
                if up_count >= Config.up_out_count:
                    # 使用随机上传
                    Config.download_index = random.randint(0, files_length -1)
                    return

                # 增加下载次数
                Utils.update_by_id(1695279857, "UP_COUNT", up_count + 1)





        temp_columns = Base.up_file_columns

        temp_columns = {
            temp_columns[0]: Utils.get_csv_ID(),
            temp_columns[1]: file_base_name,
            temp_columns[2]: file_path,
            temp_columns[3]: file_size_mb,
            temp_columns[4]: Base.now_up_count,
            temp_columns[5]: Utils.get_formatter_datetime_now(),
            temp_columns[6]: "TRY",
            temp_columns[7]: "FALSE",
        }


        # 执行成功前写入文件中
        Utils.writer_csv(temp_columns)

        # 默认执行命令

        # 主播名称
        title_index = file_base_name.index("2")
        title = file_base_name[:title_index]



        # 获取当前日期和时间
        current_datetime = datetime.now()

        # 提取当前月份
        current_month = current_datetime.month.__str__() + "月"

        # 年份
        current_year = current_datetime.year.__str__() + "年"

        # 'telegram-upload --caption "%s      #%s #%s" --to https://t.me/LuBozhan "%s"'
        exe_command = Config.cmd_command % (file_base_name, title, current_year + current_month, file_path)
        result = subprocess.run(exe_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=60 * 60 * 4)

        if result.returncode == 0:
            # 写完完成修改STATUS
            Utils.update_by_id(temp_columns["ID"], "STATUS", "SUCCEED")
            Utils.log_current_thread_and_datetime("INFO", f"FILE : {file_base_name} , 上传服务器成功", Config.log_folder)

        # 删除上传成功的文件
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                # 执行到此处修改文件IS_DELETE
                Utils.update_by_id(temp_columns["ID"], "IS_DELETE", "TRUE")
                Utils.log_current_thread_and_datetime("INFO", f"FILE : {file_base_name} , 成功被删除",
                                                      Config.log_folder)


            except OSError as error:
                # 执行到此处修改文件IS_DELETE
                Utils.update_by_id(temp_columns["ID"], "IS_DELETE", "FALSE")
                Utils.update_by_id(temp_columns["ID"], "MESSAGE", error)
                Utils.log_current_thread_and_datetime("ERROR",
                                                      f"FILE : {file_base_name} , 删除异常 , {error} : {Utils.traceback_to_str(error)}",
                                                      Config.log_folder)
                Base.now_up_count = Base.now_up_count + 1

        if not os.path.exists(file_path):
            # 执行到此处修改文件IS_DELETE
            Utils.update_by_id(temp_columns["ID"], "IS_DELETE", "TRUE")




# 入口函数
if __name__ == '__main__':
    # 程序开始时会扫描一次目录
    Base.files = glob.glob((Config.scan_folder_path + "/*"))

    while 1:
        try:
            main()
        except Exception as e:
            time.sleep(5)
            print(e)



    # # 删除任务
    # scheduler.remove_job('1_seconds')
    # # 暂停任务
    # scheduler.pause_job('2022_date')
    # scheduler.pause_job('6_cron')
    # # 恢复任务
    # scheduler.resume_job('6_cron')
    # # 修改任务
    # scheduler.modify_job(job_id='2022_date', jobstore=None)
    # # 修改单个作业的触发器并更新下次运行时间
    # result = scheduler.reschedule_job(job_id='6_cron', trigger='interval', seconds=10)

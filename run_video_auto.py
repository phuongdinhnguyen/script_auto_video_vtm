import json
import csv
import psutil
import subprocess
import time
import os

#--------------------Mở và lấy tham số các file--------------------# 
with open("config.json", "r") as jsonfile:
    config = json.load(jsonfile)
    print("Read successful")

run_parallel = config['runParallel']            # cờ: cho phép chạy song song các video
configFile_8bit = config['configFile8bit']      # file config chung cho các video 8 bit
configFile_10bit = config['configFile10bit']    # file config chung cho các video 10 bit
executionFile = config['executionFile']         # tên của file exe chạy video

videoFolderDir = config['videoFolderDir']       # đường dẫn đến folder chứa các video
videoList = config['videoList']                 # đường dẫn đến file csv chứa thông số các video
runFullFrame = config['runFullFrame']           # cờ: chạy cả video (full frame)

qp_list = config['qp_list']                     # danh sách các giá trị qp

#--------------------Đọc danh sách video và các arguments cần dùng --------------------# 
videosArgs = []
with open(videoList, 'r', encoding='utf-8-sig') as file:
    videoInfo = list(csv.reader(file))
    props = videoInfo[0]
    for i in range(1, len(videoInfo)):
        info = videoInfo[i]
        args = {}
        idx = 0
        for prop in info:
            args[props[idx]] = info[idx]
            idx += 1
        videosArgs.append(args)

#--------------------Tạo các command line (chạy video qua cmd)--------------------#
def toCommand(args):
    cmd = ""
    for arg in args:
        cmd += arg + " "
    return cmd

listCommand = []
cfgList = []
for Args in videosArgs:
    cmd = [executionFile]       # cmd: list các parameters trong câu lệnh
    cmd.append("-i")            # vị trí của video
    cmd.append(videoFolderDir+"\\"+Args["foldername"]+"\\"+Args["filename"])

    if (runFullFrame):          # số lượng frame sẽ chạy
        cmd.append("-f")        # --- nếu cờ runFullFrame bật, sẽ chạy toàn bộ số frame của video
        cmd.append(Args["FramesToBeEncoded"])
    else:
        cmd.append("-f")        # --- ngược lại, chỉ chạy một số lượng frame đã chỉ định
        cmd.append(Args["frameinput"])

    cmd.append("-fr")           # framerate của video
    cmd.append(Args["framerate"])

    cmd.append("-wdt")          # chiều dài video
    cmd.append(Args["width"])
    cmd.append("-hgt")          # chiều cao video
    cmd.append(Args["height"])
   
    cmd.append("-c")
    _cmd = cmd
    if (Args["bitdepth"] == "8"):               # chạy file config tương ứng với từng bitrate (thông thường là 8 và 10)
        for cfgFile in configFile_8bit:
            _cmd.append(cfgFile)
            listCommand.append(toCommand(_cmd))
            cfgList.append(cfgFile)
            _cmd.pop()
    else:
        for cfgFile in configFile_10bit:
            _cmd.append(cfgFile)
            listCommand.append(toCommand(_cmd))
            cfgList.append(cfgFile)
            _cmd.pop()

    #----- thêm các parameters khác tại vị trí này -----#

    ### Cách 1
    # cmd.append("-q")        # dòng 1
    # cmd.append(Args["qp"])  # dòng 2

    ### Cách 2
    # cmd.append("-q")            # option chạy trong phần mềm
    # _cmd = cmd
    # for qp in qp_list:          # danh sách các qp lấy ra trong file config
    #     _cmd.append(str(qp))    # tạo câu lệnh với từng giá trị qp
    #     listCommand.append(toCommand(_cmd))
    #     _cmd.pop()

    #---------------------------------------------------#



#--------------------Sinh ra danh sách các core--------------------#
cpu_count = psutil.cpu_count()
core_count = int(cpu_count / 2)
if (core_count + 1 < 3):
    print("Not recommend running on pc with 2 cores only.")
    exit()
    
print("Number of logical cpu: ",cpu_count)
cpu_idx = 0
system_core = []
for i in range(0,int(cpu_count/2)):     # sinh ra danh sách các cpu, để chạy đa luồng
    core = {}
    core["isUsed"] = 0
    core["cpu"] = [cpu_idx, cpu_idx+1]
    system_core.append(core)
    cpu_idx += 2

system_core[0]['isUsed'] = 1            # mặc định để core đầu tiên được nghỉ

#--------------------Bắt đầu tạo ra các process để chạy--------------------#
def countFree(system_core):             # đếm số core free
    cnt = 0
    for core in system_core:
        if (core['isUsed'] == 0): cnt += 1
    return cnt

def findAvailCPU(system_core):          # core còn trống chưa chạy
    for core in system_core:
        if (core['isUsed'] == 0): return core['cpu']

def checkPID(_pid):                     # hàm này kiểm tra xem pid có tồn tại không
    try:
        p = psutil.Process(_pid)
    except:
        # print("process not found!")
        return -1

    # print(p.name())

clear = lambda: os.system('cls')
running_queue = []
max_video = core_count - 1

while True:                             # vòng lặp vô tận, liên tục kiểm tra trạng thái các video  
    clear()
    print("List of running videos: (core_number - video_name - pid - config file)")
    print("Number of runnning videos:",len(running_queue))
    if (len(running_queue) > 0):
        for item in running_queue:
            print(item["thread_run"]," ",item["video_name"]," ",item["pid"]," ",item["config_file"])
            print(item["command"])
        time.sleep(3)

    if (len(running_queue) == 0):     # thoát chương trình khi tất cả các video đã chạy xong
        print("finished running all videos!")
        break

    if (len(running_queue) > 0):
        for item in running_queue:
            pid = item["pid"]
            core_run = int(item['thread_run'][0] / 2)
            print("checking core:",core_run)
            if (checkPID(pid) == -1):
                # trả lại core
                system_core[core_run]["isUsed"] = 0
                print("kick queue core:",core_run)
                # đá khỏi queue
                running_queue.pop(running_queue.index(item))

    if (len(listCommand) == 0): continue
    if (len(listCommand) == 0): continue
    if (len(running_queue) == max_video): continue

    availCPU = findAvailCPU(system_core)
    runCommand = listCommand.pop(0)
    before_pids = psutil.pids()     # lấy danh sách các pid lúc trước
    x = subprocess.Popen(runCommand, shell=True)        # đẩy command đi chạy
    time.sleep(1)                                       # chờ 1s để command chạy
    system_core[int(availCPU[0]/2)]["isUsed"] = 1
    after_pids = psutil.pids()      # lấy danh sách các pid lúc sau

    pid_list = list(set(after_pids) - set(before_pids)) # lấy danh sách pid sai khác để check các process mới

    Arg = runCommand.split(' ')
    video_info = {"command":runCommand,
                "video_name":Arg[2],
                "config_file":cfgList.pop(0),
                "thread_run":availCPU} 

    for pid in pid_list:
        try:                        # try catch đề phòng process chạy video bị lỗi
            p = psutil.Process(pid)
        except:
            print("PID does not exist! Please check config/system again!")
        # thêm pid vào dict của video
        if (p.name() == "EncoderApp.exe"):
            video_info["pid"] = pid
            # 4.3 set affinity
            p.cpu_affinity(video_info["thread_run"])                   
            break

    running_queue.append(video_info)
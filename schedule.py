import subprocess
import sys


def process(command):
    pro = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    pro.wait()
    if pro.returncode == 0:
        print("run successfull")
    else:
        print("Run unsuccessfull")
        sys.exit(1)


while True:
    process("C:\\Users\\sree\\PycharmProjects\\pythonProject1\\venv\\Scripts\\python.exe C:\\Users\\sree\\PycharmProjects\\pythonProject1\\client_monitor.py %*")

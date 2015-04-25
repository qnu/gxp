
import os

pid = os.fork()
if pid == 0:
    os.execvp("python", [ "python", "./io.py" ])

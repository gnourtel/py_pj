import threading
import time

def job():
    try:
      while True:
            print(1)
            time.sleep(1)
    except KeyboardInterrupt:
        print('err')    
# try:
a = threading.Thread(target=job, daemon=True)
a.start()
a.join()
# except KeyboardInterrupt:
#     print('err')
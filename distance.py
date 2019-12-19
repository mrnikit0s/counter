import RPi.GPIO as GPIO
from datetime import datetime
import time
 
GPIO.setmode(GPIO.BCM)
 
#Set trigger and echo GPIO
GPIO_TRIGGER = 23
GPIO_ECHO = 24
 
GPIO.setup(GPIO_TRIGGER, GPIO.OUT)
GPIO.setup(GPIO_ECHO, GPIO.IN)
 
def distance():
    # Send impulse on trigger 
    GPIO.output(GPIO_TRIGGER, True)
    time.sleep(0.000001)
    GPIO.output(GPIO_TRIGGER, False)
    # Init time var
    StartTime = time.time()
    StopTime = time.time()
 
    # save time send signal
    while GPIO.input(GPIO_ECHO) == 0:
        StartTime = time.time()
	print ("Start time: %.1f" % StartTime)
 
    # save time recieved signal
    while GPIO.input(GPIO_ECHO) == 1:
        StopTime = time.time()
	print ("Stop time: %.1f" % StopTime)
 
    # calc time
    Time = StopTime - StartTime
    # 
    distance = (Time * 34300) / 2
 
    return distance
def count ():
	# date 
	now = datetime.now()
	date = datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")
	
    return date

if __name__ == '__main__':
	
    dist = distance()
    print ("Distance = %.1f cm" % dist)
    dat = count()
    print ("Date: " % dat)
    GPIO.cleanup()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
import RPi.GPIO as GPIO
from datetime import datetime
import time
 
GPIO.setmode(GPIO.BCM)
 
# Задаем выходы GPIO для триггера и эхо
GPIO_TRIGGER = 23
GPIO_ECHO = 24
 
GPIO.setup(GPIO_TRIGGER, GPIO.OUT)
GPIO.setup(GPIO_ECHO, GPIO.IN)
 
def distance():
    # Посылаем импульс
    GPIO.output(GPIO_TRIGGER, True)
    # Время задержки (если расстояние > 3 метров - выставляем время 25-30 мсек)	
    time.sleep(0.03)
    GPIO.output(GPIO_TRIGGER, False)
    # Инициализация переменных времени
    StartTime = time.time()
    StopTime = time.time()
 
    # Сохраняем время отправки сигнала
    while GPIO.input(GPIO_ECHO) == 0:
        StartTime = time.time()
#	print ("Start time: %.1f" % StartTime)
 
    # Сохраняем время получения сигнала
    while GPIO.input(GPIO_ECHO) == 1:
        StopTime = time.time()
#	print ("Stop time: %.1f" % StopTime)
 
    # Вычисляем время
    Time = StopTime - StartTime
    # Вычисляем расстояние 
    distance = (Time * 34300) / 2
 
    return distance



def delta_distance():
    # Порог срабатывания ( процент от усреднённого расстояния )
	prcnt = 10
    # Вычисляем среднее расстояние до объекта (50 замеров)
	avrg = 0
	for i in range(50):
		dst = distance()
		avrg += dst
	avrg = avrg/50
	return avrg

# ф-ция счетчик
def count():
	delta = delta_distance()
	count = 0
	# Вычисляем границы отклонений
#	#for i in range(100):
#		dest = distance()
#		time.sleep(0.001)
#		print(dest)
#				
#	except KeyboardInterrupt:
#		print("Stopped")


if  __name__ == '__main__':
#	count()


	avrg = delta_distance()
	print (avrg)

#	for i in range(10):
#		avg = delta_distance()
#		print (avg)
#		dist = distance()
#    		print ("Distance = %.1f cm" % avg)
#    dat = count()
#    print (dat) выаыва
	GPIO.cleanup()

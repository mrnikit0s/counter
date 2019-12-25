#!/usr/bin/env python
# -*- coding: utf-8 -*-
import RPi.GPIO as GPIO
from datetime import datetime
import time
 
GPIO.setmode(GPIO.BCM)
 
# Задаем выходы GPIO для триггера и эхо
GPIO_TRIGGER = 23
GPIO_ECHO = 24
GPIO_LED = 17

GPIO.setup(GPIO_LED, GPIO.OUT) 
GPIO.setup(GPIO_TRIGGER, GPIO.OUT)
GPIO.setup(GPIO_ECHO, GPIO.IN)
GPIO.setup(GPIO_LED, False)
 
def distance():
    # Посылаем импульс
    GPIO.output(GPIO_TRIGGER, True)
    # Время задержки (если расстояние > 3 метров - выставляем время 25-30 мсек)	
    time.sleep(0.02)
    GPIO.output(GPIO_TRIGGER, False)
    # Инициализация переменных времени
    StartTime = time.time()
    StopTime = time.time()
 
    # Сохраняем время отправки сигнала
    while GPIO.input(GPIO_ECHO) == 0:
        StartTime = time.time()
 
    # Сохраняем время получения сигнала
    while GPIO.input(GPIO_ECHO) == 1:
        StopTime = time.time()
 
    # Вычисляем время
    Time = StopTime - StartTime
    # Вычисляем расстояние 
    distance = (Time * 34300) / 2
 
    return distance

def led():
	GPIO.output(GPIO_LED, True)
	time.sleep(0.2)
	GPIO.output(GPIO_LED, False)

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
def count(avrg):
	count = 0
	timeList = []
	
	min = avrg-(avrg*0.10)
	max = avrg+(avrg*0.10)
	# флаг перекрытия счетчика. по умолчанию - нет перекрытия
	trig = 0
	try:
		while True:
			delta = delta_distance()
			if (delta < min) or (delta > max):
				trig = 1
			# ловим момент, когда прекратилось перекрытие, меняем тригер на 0 и считаем человека
			if (trig == 1):
				timeList.append(time.time())
				delta_time = timeList[len(timeList)-1] - timeList[0]
				if (delta_time > 20):
					print("Error!!!")
					trig = 0
					timeList = []
				if (delta > min) and (delta < max):
					led()
					count += 1
					trig = 0
					timeList = []
					print("Count = ", count)
						
			time.sleep(0.01)	
	except KeyboardInterrupt:
		print("Stopped")


if  __name__ == '__main__':

	avrg = delta_distance()
	count(avrg)
	print (avrg)
	GPIO.cleanup()

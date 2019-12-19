#!/usr/bin/python
# -*- coding: utf-8

import RPi.GPIO as GPIO
import os
import re
import md5
import sys
import Queue
import string
import MySQLdb
import datetime
import time
import logging
import threading

import smtplib
from smtplib import SMTP_SSL
from email.mime.text import MIMEText


from time import sleep
from urlparse import urlparse
Client = None
HTTPBasicAuth = None
Transport = None

# TO DO --------------------------------------------------------
# Послать сообщение при длительном отсутствии изменений в рабочее время

# SETTINGS -----------------------------------------------------
reload(sys)
sys.setdefaultencoding('utf8')

node_name = ''			# Наименование узла в 1С
threshold_ms = 200		# Чувствительность 200мс
closedetect_ms = 120000 # Определение перекрытия 2 мин

wsdl_url = 'http://78.46.34.221:8080/test2/ws/counter.1cws?WSDL'
user_login = ''
user_password = ''

sendmess_url = 'http://78.46.34.221:8080/test2/ws/sendmessage.1cws?WSDL'

mts_soap = 'http://mcommunicator.ru/m2m/m2m_api.asmx'
mts_user = ''
mts_pass = ''
mts_wsdl = ''
phones = ''		# Могут быть через запятую 7919xxxxxxx

schedule = ''	# Расписание 1(8-19),2(10-15:30)
period_s = 1800	# Выравнивание по 30 минутам

#email
address_from = "counter@instrument.ms"
smtp_password="igAkOWzi2JMDARIPI17M"
address_to = "it.notification@instrument.ms"



# Настройки скрипта --------------------------------------------
flash_led_period_s = 1 	# Мигаем раз в 1 сек, при отсутствии сигнала
IR_interrupt_ms = 1000 	# Когда сигнал отсутствует более 1 сек, изменение статуса Линии происходит не чаще этого параметра
max_sms_length = 140	# Максимальное количество символов в одной СМС
max_latency_ms = 20		# Хороший сигнал чередуется с частотой менее
max_queue_size = 50		# Сколько раз подряд хороший сигнал считается хорошей линией
max_bad_signal_time_s = 120		# В течении какого времени должен продержатся плохой сигнал, для установки флага BAD_LINE
mess_close = 'Датчик перекрыт на узле подсчета посетителей '
mess_badsignal = 'Плохой сигнал датчика на узле подсчета посетителей '
mess_linenormal = 'Счетчик снова работает нормально на узле подсчета посетителей '
mess_badppl = 'Во время отсутствия интернета отсутствовал сигнал счетчика на узле '
detect_badppl = True 	# Слать СМС при отсутствии сигнала и отсутствии интернета
# --------------------------------------------------------------

LINE_EXISTS = False		# По умолчанию нет соединения с датчиком ИР
BAD_LINE = False 		# Флаг плохой линии
thread_error = False 	# Глобальный флаг остановки процесса

fLEDflasher = False 	# Признак запущенного потока
kill_LEDflasher = False # Флаг для удаления потока

# Потоки -------------------------------------------------------
tLEDflasher = None
tSendData = None
# --------------------------------------------------------------
PortQueue = None 		# Очередь для обработки сообщений порта
queue_thread = False 	# Флаг потока обработки сообщений
detect_thread = False 	# Флаг потока определения наличия сети

# sleep(300)

iLock = threading.Lock()
detect_event = None 	# Событие наличия поиска сети

loadtime = datetime.datetime.now() 	# Время старта скрипта

current_schedule = {'status':False, 'timefrom':loadtime, 'timeto':loadtime}
current_count = 0
current_last = loadtime
current_lastvalue = 0

firsttime = True

def iClient(status):
	global Client
	global Transport
	global HTTPBasicAuth
	global firsttime

	iLock.acquire()
	try:
		skipdetect = status and (not Client is None)

		if status:
			if Transport is None:
				_temp = __import__('zeep.transports', globals(), locals(), ['Transport'], -1)
				Transport = _temp.Transport
			if HTTPBasicAuth is None:
				_temp = __import__('requests.auth', globals(), locals(), ['HTTPBasicAuth'], -1)
				HTTPBasicAuth = _temp.HTTPBasicAuth
			if Client is None:
				_temp = __import__('zeep', globals(), locals(), ['Client'], -1)
				Client = _temp.Client

				skipdetect = skipdetect or firsttime
		else:
			Client = None
			Transport = None
			HTTPBasicAuth = None

		# Определение мощенничества, если нет интернета и плохая линия или линия отсутствует
		# Необходимо сообщить об этом когда, интернет появится
		if not skipdetect:
			#if ((not LINE_EXISTS) or BAD_LINE) and ((datetime.datetime.now() - loadtime).total_seconds() >= 180): # Со времени загрузки прошло более 3 минут
			# Строка для теста
			if (not LINE_EXISTS) or BAD_LINE:
				if detect_badppl:
					detect_event.set()
	except:
		Client = None
		Transport = None
		HTTPBasicAuth = None
	finally:
		iLock.release()

def LED(status):
    GPIO.output(17, status)

def LEDflasher(interval, sleep_interval):
    global thread_error
    global tLEDflasher
    global fLEDflasher

    logging.info('LED flasher started')
    fLEDflasher = True
    while not kill_LEDflasher:
    	try:
    		if not LINE_EXISTS:		# Нет сигнала, мигаем
    			LED(1)
			#executeSQL("INSERT INTO ledstatus (data,led) VALUES ('%s',1)" % (time.strftime('%Y-%m-%d %H:%M:%S')), False, True)
        		sleep(sleep_interval)
        		LED(0)
        	elif BAD_LINE:
        		LED(1)
			#executeSQL("INSERT INTO ledstatus (data,led) VALUES ('%s',1)" % (time.strftime('%Y-%m-%d %H:%M:%S')), False, True)
    			sleep(interval)
        		LED(0)
        	else:
        		LED(1)
			#executeSQL("INSERT INTO ledstatus (data,led) VALUES ('%s',1)" % (time.strftime('%Y-%m-%d %H:%M:%S')), False, True)
        		sleep(interval / 2)
        		LED(0)
        		sleep(sleep_interval * 3)
        	sleep(sleep_interval)
        except Exception as excep:
        	tLEDflasher = None
        	thread_error = True
        	logging.error('LEDFlasher THREAD: ' + str(excep))
        	break
    logging.info('LED flasher stopped')
    fLEDflasher = False

# Налагаем ограничение на лог-файл, приблизительно 10Мб
try:
	logsize = os.path.getsize('counter.log')
except:
	logsize = 0

if logsize > 10000000:
	os.remove('counter.log')
# -----------------------------------------------------

# LOGGING SYSTEM
logging.basicConfig(level=logging.INFO, format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', filename='counter.log')

gLock = threading.Lock()
sLock = threading.Lock()
cursor_cnt = None
db = None

def executeSQL(script, fetch, commit):
	global thread_error

	data = None
	gLock.acquire()
	try:
		if not cursor_cnt is None:
			cursor_cnt.execute(script)
			if fetch:
				data = cursor_cnt.fetchall()
			if commit and not db is None:
				db.commit()
	except Exception as err:
		logging.critical('SQL ERROR: ' + script)
		logging.critical(str(err))

		thread_error = True
	finally:
		gLock.release()
	return data

def sendMail(mess):
	global address_from
	global address_to
	global smtp_password
	print type(mess),mess
	msg = MIMEText(mess.encode('cp1251'))
	msg['From'] = address_from
	msg['To'] = address_to
	msg['Subject'] = mess
	# Send mail
	smtp = SMTP_SSL()
	smtp.connect('smtp.yandex.ru')
	smtp.login(address_from, smtp_password)
	smtp.sendmail(address_from, address_to, msg.as_string())
	smtp.quit()




def sendSmsAndLOG(mess):
	global mts_user
	global mts_pass

	sLock.acquire()
	try:
		iClient(True)

		srv_sendmess = None
		if (len(sendmess_url) > 0) and (len(user_login) > 0) and (len(user_password) > 0) and (len(mess) <= max_sms_length):
			for match in re.finditer(u'(?:[,]|^)[ ]*(?P<user1C>[а-яА-Яa-zA-Z0-9_ёЁ]*)[ ]*[(][ ]*(?P<userWIN>[а-яА-Яa-zA-Z0-9_ёЁ\\\\]*)[ ]*[)][ ]*(?:[,]|$)', phones.decode('utf8'), re.S):
				if len(match.group('user1C')) + len(match.group('userWIN')) > 0:
					if srv_sendmess is None:
						srv_sendmess = Client(sendmess_url, transport=Transport(http_auth=HTTPBasicAuth(user_login, user_password))) 	# Внутренний клиент для отправки 1С сообщений
					res = srv_sendmess.service.SendMessage(0, match.group('user1C'), match.group('userWIN'), 0, 'Администратор', '', mess)
					if re.match('^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$', res):
						pass
					else:
						logging.warning('Cant send 1C message: (%s). Address = <%s>-<%s>' % (res, match.group('user1C'), match.group('userWIN')))

		# Создаем сервис для отправки СМС администраторам узла
		if (mts_soap != '') and (not mts_soap is None):
			if mts_wsdl is '':
				client = Client(mts_soap, transport=Transport(http_auth=HTTPBasicAuth(user_login, user_password))) 		# Внутренний клиент отправки СМС
			else:
				#client = Client(mts_wsdl, location=mts_soap) 								# Клиент для МТС Коммуникатора
				client = None
		else:
			mts_user = ''
			mts_pass = ''

		if (not mts_user is None) and (not mts_pass is None):
			if (len(mts_user) > 0) and (len(mts_pass) > 0) and (len(mess) <= max_sms_length):
				is_mts = True
				try:
					mts_parse = urlparse(mts_soap)
					s1c_parse = urlparse(sendmess_url)
					if mts_parse.netloc == s1c_parse.netloc:
						is_mts = False
				except:
					pass
				
				for match in re.finditer('(?:[, ]*[7]|^[ ]*[7])(?P<phone>[0-9]{10})[ ]*(?:[,]|$)', phones, re.S):
					if is_mts:
						mess_id = client.service.SendMessage('7' + match.group('phone'), mess, 'Instrument', mts_user, mts_pass)
						if mess_id <= 0:
							logging.warning('Cant send SMS to (%s) <%s>' % ('7' + match.group('phone'), mess_id))
					else:
						send_res = client.service.SendSMS('7' + match.group('phone'), mess)
						if send_res != '+':
							logging.warning('Cant send SMS to (%s) <%s>' % ('7' + match.group('phone'), send_res))
	except Exception as err:
		logging.warning("SMS SEND: " + str(err))
		# Если не удалось проинициализировать интернет клиент
		iClient(False)
	finally:
		sLock.release()

	logging.info(mess)
	sendMail(mess)

# Функции о перекрытии датчика ---------------------------------------
# --------------------------------------------------------------------
last_closedetect = None
is_closed = False


def close_detected():
	global last_closedetect
	global is_closed
	last_closedetect = datetime.datetime.now().date()
	outfile = open("line.txt", "w")
	outfile.write("0")
	outfile.close() #Пишем в файл значение LINE_EXIST=false 
	sendSmsAndLOG(mess_close + node_name)
	is_closed = True

def badline_detected():
	outfile = open("badline.txt", "w")
	outfile.write("1")
	outfile.close() #Пишем в файл значение BAD_LINE=true 
	sendSmsAndLOG(mess_badsignal + node_name)

def badline_normalized():
	outfile = open("badline.txt", "w")
	outfile.write("0")
	outfile.close() #Пишем в файл значение BAD_LINE=false 

#Пишем в файл значение LINE_EXIST
def line_state_tofile(state):  
	outfile = open("linestatus.txt", "w")
	outfile.write(state)
	outfile.close() 
#Пишем в файл значение BAD_LINE
def badline_state_tofile(state):  
	outfile = open("badlinestatus.txt", "w")
	outfile.write(state)
	outfile.close() 

def line_normalized():
	global is_closed
	if is_closed:
		is_closed = False
		sendSmsAndLOG(mess_linenormal + node_name)
		outfile = open("line.txt", "w")
		outfile.write("2")
		outfile.close() #Пишем в файл значение LINE_EXIST=true 


# --------------------------------------------------------------------
# --------------------------------------------------------------------
# --------------------------------------------------------------------

def check_diffr(diff_ms, currdate):
	global current_count
	global current_last

	if thread_error:
		return
	if diff_ms >= threshold_ms:	# Это не наводка
		# Update data
		bod = datetime.datetime(currdate.year, currdate.month, currdate.day, 0, 0, 0, 0) # Begin of the day
		add_sec = (currdate - bod).total_seconds() // period_s * period_s

		signal_data = bod + datetime.timedelta(seconds=add_sec)
		found = False

		data = executeSQL("SELECT * FROM count WHERE data = '%s'" % (signal_data.strftime('%Y-%m-%d %H:%M:%S')), True, False)
		if thread_error:
			return

		rec_count = 0
		rec_countin = 0
		rec_countout = 0
		rec_rest = 0

		for rec in data:
			found = True
			rec_id, rec_data, rec_count, rec_sent, rec_countin, rec_countout, rec_rest = rec
			break

		rec_count += 1;

		data = executeSQL("SELECT rest, data FROM count WHERE count_in <> count_out and data >= '%s' and data < '%s' ORDER BY data DESC LIMIT 1" % (bod.strftime('%Y-%m-%d %H:%M:%S'), signal_data.strftime('%Y-%m-%d %H:%M:%S')), True, False)
		if thread_error:
			return
		for rec in data:
			rec_rest, rec_lastrec = rec
			break

		# rec_rest = 0 (следующая запись должно больше войти), rec_rest = 1 (должно больше выйти)
		if rec_rest == 0:
			rec_countin = rec_count // 2 + rec_count % 2
			rec_countout = rec_count // 2
			if rec_countin != rec_countout:
				rec_rest = 1
		else:
			rec_countin = rec_count // 2
			rec_countout = rec_count // 2 + rec_count % 2
			if rec_countin != rec_countout:
				rec_rest = 0

		if found:		# Update
			executeSQL("UPDATE count SET countnum = %s, sent = %s, count_in = %s, count_out = %s, rest = %s WHERE id = %s" % (rec_count, 0, rec_countin, rec_countout, rec_rest, rec_id), False, True)
		else:			# Insert
			executeSQL("INSERT INTO count (data, countnum, sent, count_in, count_out, rest) VALUES ('%s', %s, %s, %s, %s, %s)" % (signal_data.strftime('%Y-%m-%d %H:%M:%S'), rec_count, 0, rec_countin, rec_countout, rec_rest), False, True)

		if current_schedule['status']:
			if (currdate >= current_schedule['timefrom']) and (currdate <= current_schedule['timeto']):
				current_count += 1
				current_last = currdate

def detect_internet():
	global detect_thread
	global thread_error

	detect_thread = True
	try:
		while detect_event.wait():
			if thread_error:
				break

			detect_event.clear()

			#sleep(60)		# Ждем минуту
			i = 0
			while i < 60:
				if thread_error:
					break
				i += 1
				sleep(1)

			if thread_error:
				break

			if Client is None:
				iClient(True)

			sendSmsAndLOG(mess_badppl + node_name)
	except Exception as err:
		logging.critical('DETECT INTERNET THREAD: ' + str(err))
		thread_error = True
	detect_thread = False

def queue_thread_prc():
	global queue_thread
	global thread_error
	global BAD_LINE

	queue_thread = True
	try:
		statistic = Queue.Queue()
		bad_signal = False
		last_bad_signal = datetime.datetime.now()
		while True:
			item = PortQueue.get(True)				# Ждем появления сообщения
			if item['diff'] is None:				# Затребован выход из потока обработки очереди
				PortQueue.task_done()
				break;
			else:
				check_diffr(item['diff'], item['datetime'])
				PortQueue.task_done()

				if item['diff'] < max_latency_ms:
					if len(statistic.queue) > 0:
						last_item = statistic.queue[len(statistic.queue) - 1]
						if (item['datetime'] - last_item['datetime']).total_seconds() * 1000 > max_latency_ms:
							statistic.queue.clear()
					bad_signal = False
					statistic.put(item, True)
					if len(statistic.queue) > max_queue_size:
						statistic.get(True)
						statistic.task_done()
						if BAD_LINE:
							badline_normalized() #Оповестить о отмене статуса плохой линии на датчике
							badline_state_tofile("0") #Пишем в файл
						BAD_LINE = False
				else:
					statistic.queue.clear()
					if not bad_signal:
						last_bad_signal = datetime.datetime.now()
					elif ((datetime.datetime.now() - last_bad_signal).total_seconds() >= max_bad_signal_time_s) and (not BAD_LINE):
						if not BAD_LINE:
							badline_state_tofile("2") # пишем в файл. 2 чтоб на графике мониторинга не пересекался с LINE_EXIST(1)
						BAD_LINE = True
						# Оповестить о появлении плохой линии на датчике
						badline_detected()
						# ----------------------------------------------
					bad_signal = True
	except Exception as err:
		logging.critical('QUEUE THREAD: ' + str(err))
		thread_error = True
	queue_thread = False

send_event = None 					# Событие завершения потока отправки
send_thread = False 				# Статус потока отправки данных

def get_wait_secs(interval_s):
	curr = datetime.datetime.now()
	bod = datetime.datetime(curr.year, curr.month, curr.day, 0, 0, 0, 0)
	add_sec = (curr - bod).total_seconds() // interval_s * interval_s
	nexttime = bod + datetime.timedelta(seconds=add_sec + interval_s)
	wait_s = (nexttime - curr).total_seconds()
	return wait_s

def parse_schedule(week_day, text_schedule):
	mask = ".*%s[ ]*[(](?P<hourfrom>[0-9]+)[:]*(?P<minutefrom>[0-9]*)[ ]*[-][ ]*(?P<hourto>[0-9]+)[:]*(?P<minuteto>[0-9]*)[)].*" % week_day
	m = re.match(mask, text_schedule)
	if m:
		try:
			hourfrom = int(m.group('hourfrom'))
			if len(m.group('minutefrom')) > 0:
				minutefrom = int(m.group('minutefrom'))
			else:
				minutefrom = 0
			hourto = int(m.group('hourto'))
			if len(m.group('minuteto')) > 0:
				minuteto = int(m.group('minuteto'))
			else:
				minuteto = 0
			status = True
		except:
			logging.warning("BAD TIME FORMAT FOR (%s) IN (%s) SHOULD BE <day>(<hour:minute>-<hour:minute>)" % (week_day, text_schedule))
			hourfrom = 0
			minutefrom = 0
			hourto = 0
			minuteto = 0
			status = False
	else:
		hourfrom = 0
		minutefrom = 0
		hourto = 0
		minuteto = 0
		status = False
	return {'status':status, 'hourfrom':hourfrom, 'minutefrom':minutefrom, 'hourto':hourto, 'minuteto':minuteto}

def send_data_to_server(srv, date, count_in, count_out, param):
	try:
		try:
			result = srv.service.SetData(date, node_name, count_in, count_out)
		except:
			# Не смогли отправить данные
			iClient(False)
			return False

		if (result == '+'):
			executeSQL("UPDATE count SET sent = true WHERE data = '%s'" % (date.strftime('%Y-%m-%d %H:%M:%S')), False, True)
			if thread_error:
				return True
			param[0] = date # rec_lastsent = date
			param[1] = True # update = True
		else:
			logging.warning("Cant sent data to server %s, data = '%s'" % (wsdl_url, date.strftime('%d.%m.%Y %H:%M:%S')))
			logging.error(result)
	except Exception as err:
		logging.error("SEND TO SERVER: " + str(err))
	return False

def sender_thread(interval_s):
	global thread_error
	global send_thread
	global current_schedule
	global current_count

	send_thread = True
	try:
		wait_s = get_wait_secs(interval_s)
		while not send_event.wait(wait_s):			# Отправка наступила
			if thread_error:
				break

			send_event.clear()

			# Получим расписание -------------------
			try:
				iClient(True)
				srv = Client(wsdl_url, transport=Transport(http_auth=HTTPBasicAuth(user_login, user_password)))
			except:
				iClient(False)
				wait_s = get_wait_secs(interval_s)		# Ждем следующей отправки
				continue
			# Если не удалось проинициализировать интернет клиент, возможно нет сети

			data = executeSQL("SELECT * FROM lastvalue", True, False)
			if thread_error:
				break

			time_now = datetime.datetime.now()
			time_bod = datetime.datetime(time_now.year, time_now.month, time_now.day, 0, 0, 0, 0)
			
			rec_weekday = 0
			week_day = time_now.isoweekday()	# 1 - 7 (Monday = 1)
			rec_lastsent = None 				# Дата последней отсылки в течении текущего дня
			update = False
			wd = 0								# День недели записанный в базе lastvalue, используется для обновления информации
			found = False
			for rec in data:
				found = True
				rec_weekday, txt_schedule, rec_lastsent = rec
				wd = rec_weekday
				rec_schedule = parse_schedule(rec_weekday, "%s(%s)" % (rec_weekday, txt_schedule))

			datewrong = False
			if rec_lastsent is None:
				datewrong = True
			elif rec_lastsent.date() != time_now.date():
				datewrong = True

			if (rec_weekday != week_day) or datewrong:
				rec_lastsent = None
				rec_schedule = parse_schedule(week_day, schedule)	# Поищем расписание на текущий день
				if (rec_weekday > 0) and (not rec_schedule['status']):	# Есть запись в базе о предыдущем расписании
					rec_schedule = parse_schedule(week_day, "%s(%s)" % (week_day, txt_schedule))
				txt_schedule = ''
				if rec_schedule['status']:							# Расписание существует
					txt_schedule = "%02d:%02d-%02d:%02d" % (rec_schedule['hourfrom'], rec_schedule['minutefrom'], rec_schedule['hourto'], rec_schedule['minuteto'])
				rec_weekday = week_day
				update = True 										# Флаг обновления базы lastvalue

			if rec_schedule['status']:
				schedule_time_from = datetime.datetime(time_bod.year, time_bod.month, time_bod.day, rec_schedule['hourfrom'], rec_schedule['minutefrom'], 0, 0)
				schedule_time_to = datetime.datetime(time_bod.year, time_bod.month, time_bod.day, rec_schedule['hourto'], rec_schedule['minuteto'], 0, 0)
				
				if (current_schedule['timefrom'] != schedule_time_from) or (current_schedule['timeto'] != schedule_time_to) or not current_schedule['status']:
					current_schedule = {'status':True, 'timefrom':schedule_time_from, 'timeto':schedule_time_to}
					current_count = 0
			else:
				schedule_time_from = time_bod
				schedule_time_to = time_bod - datetime.timedelta(seconds = 1)

				current_schedule = {'status':False, 'timefrom':schedule_time_from, 'timeto':schedule_time_to}
				current_count = 0

			data = executeSQL("SELECT * FROM count WHERE not sent ORDER BY data", True, False)
			if thread_error:
				break

			dataempty = True
			for rec in data:
				rec_id, rec_date, rec_count, rec_sent, rec_countin, rec_countout, rec_rest = rec
				dataempty = False
				# ------------------------------------------------------------
				if rec_date >= time_bod:			# Текущий день
					if not rec_lastsent is None:	# Были уже отсылки за этот день
						first_edge = time_bod + datetime.timedelta(seconds=(rec_lastsent - time_bod).total_seconds() // period_s * period_s + period_s)
						while first_edge < rec_date:
							# Если за пределами границы расписания, нули не надо добавлять
							if rec_schedule['status']:
								if (first_edge > schedule_time_to) or (first_edge < schedule_time_from):
									first_edge += datetime.timedelta(seconds = period_s)
									continue
							# Добавим "ноль" в промежуток, кратный периоду
							zero_data = executeSQL("SELECT * FROM count WHERE data = '%s'" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), True, False)
							z_found = False
							z_countin = 0
							z_countout = 0
							for z_rec in zero_data:
								z_id, z_date, z_count, z_sent, z_countin, z_countout, z_rest = z_rec
								z_found = True
							if z_found:
								executeSQL("UPDATE count SET sent = false WHERE data = '%s'" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), False, True)
							else:
								executeSQL("INSERT INTO count (data) VALUES ('%s')" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), False, True)
							if thread_error:
								break
							# Отправим "ноль" на сервер
							param = [rec_lastsent, update]
							if send_data_to_server(srv, first_edge, z_countin, z_countout, param):
								break
							elif Client is None:
								break
							else:
								rec_lastsent, update = param
							# --------------------------------------------
							first_edge += datetime.timedelta(seconds = period_s)
						if thread_error:
							break
				# ------------------------------------------------------------
				if Client is None:
					break

				param = [rec_lastsent, update]
				if send_data_to_server(srv, rec_date, rec_countin, rec_countout, param):
					break
				elif Client is None:
					break
				else:
					rec_lastsent, update = param

				if rec_date >= time_bod:
					rec_lastsent = rec_date
					update = True

			if Client is None:
				wait_s = get_wait_secs(interval_s)		# Ждем следующей отправки
				continue

			if thread_error:
				break

			# ------------------------------------------------------------------------------------
			if dataempty:				# Не было данных в выборке - заполним нулями по расписанию
				scale_zero = False
				time_part = time_bod + datetime.timedelta(seconds = (time_now - time_bod).total_seconds() // period_s * period_s)
				if (time_now >= schedule_time_from) and (time_now <= schedule_time_to):
					scale_zero = True
					if rec_lastsent is None:
						first_edge = schedule_time_from
					else:
						first_edge = rec_lastsent
				if scale_zero:
					while first_edge <= time_part:
						# Если за пределами границы расписания, нули не надо добавлять
						if rec_schedule['status']:
							if (first_edge > schedule_time_to) or (first_edge < schedule_time_from):
								first_edge = time_bod + datetime.timedelta(seconds = (first_edge - time_bod).total_seconds() // period_s * period_s + period_s)
								continue
						# Добавим "ноль" в промежуток, кратный периоду
						zero_data = executeSQL("SELECT * FROM count WHERE data = '%s'" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), True, False)
						z_found = False
						z_countin = 0
						z_countout = 0
						for z_rec in zero_data:
							z_id, z_date, z_count, z_sent, z_countin, z_countout, z_rest = z_rec
							z_found = True
						if z_found:
							executeSQL("UPDATE count SET sent = false WHERE data = '%s'" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), False, True)
						else:
							executeSQL("INSERT INTO count (data) VALUES ('%s')" % (first_edge.strftime('%Y-%m-%d %H:%M:%S')), False, True)
						if thread_error:
							break
						# Отправим "ноль" на сервер
						param = [rec_lastsent, update]
						if send_data_to_server(srv, first_edge, z_countin, z_countout, param):
							break
						elif Client is None:
							break
						else:
							rec_lastsent, update = param
						# --------------------------------------------
						first_edge = time_bod + datetime.timedelta(seconds = (first_edge - time_bod).total_seconds() // period_s * period_s + period_s)

					if Client is None:
						wait_s = get_wait_secs(interval_s)		# Ждем следующей отправки
						continue
			# ------------------------------------------------------------------------------------

			if update and not rec_lastsent is None:					# Были отсылки, необходимо обновить базу lastvalue
				if found:
					executeSQL("UPDATE lastvalue SET weekday = %s, schedule = '%s', lastsent = '%s' WHERE weekday = %s" % (rec_weekday, txt_schedule, rec_lastsent.strftime('%Y-%m-%d %H:%M:%S'), wd), False, True)
				else:
					executeSQL("INSERT INTO lastvalue (weekday, schedule, lastsent) VALUES (%s, '%s', '%s')" % (rec_weekday, txt_schedule, rec_lastsent.strftime('%Y-%m-%d %H:%M:%S')), False, True)
			if thread_error:
				break

			wait_s = get_wait_secs(interval_s)		# Ждем следующей отправки
	except Exception as err:
		logging.critical("SENDER THREAD: " + str(err))
		thread_error = True
	send_thread = False

# Чтение начальных данных
qthr = None
sthr = None
dthr = None
try:
	# Создаем очередь для обработки сообщений порта
	PortQueue = Queue.Queue()

	send_event = threading.Event()		# Флаг завершения потока отправки данных
	send_event.clear()

	detect_event = threading.Event()	# событие наличия поиска сети
	detect_event.clear()

	# подключаемся к базе данных (не забываем указать кодировку, а то в базу запишутся иероглифы)
	db = MySQLdb.connect(host="localhost", user="ir_counter", passwd="c1", db="ir_counter", charset='utf8')
	cursor_cnt = db.cursor()	# Указатель на таблицу данных

	settings = executeSQL("SELECT * FROM settings", True, False)

	for data in settings:
		node_name, threshold_ms, closedetect_ms, period_s, schedule, wsdl_url, user_login, user_password, mts_soap, mts_user, mts_pass, phones = data
		break

	# Make MD5 HASH for MTSCommunicator
	if (not mts_pass is None) and (mts_pass != ''):
		mts_pass = md5.new(mts_pass).hexdigest() 	# Рассчитаем ХЭШ для МТС Коммуникатора

	if mts_soap.upper().endswith('?WSDL'):
		mts_wsdl = '' 					# Внутренняя отправка СМС
	else:
		mts_wsdl = mts_soap + '?WSDL' 	# Отправка через МТС Коммуникатор

	# Получим адрес интерфейса для отправки 1С сообщений
	sendmess_url = ''
	if len(wsdl_url) > 0:
		try:
			u_path = ''
			u_parse = urlparse(wsdl_url)
			if len(u_parse.path) > 0:
				try:
					s1 = u_parse.path.rindex(r'/')
				except:
					s1 = -1
				try:
					s2 = u_parse.path.rindex('\\')
				except:
					s2 = -1
				u_path = u_parse.path[0 : max(s1, s2) + 1]
			else:
				u_path = ''
			if not u_path is '':
				sendmess_url = u_parse.scheme + r'://' + u_parse.netloc + u_path + 'sendmessage.1cws?' + u_parse.query
		except:
			sendmess_url = ''
			logging.warning('URL PARSE ERROR: (%s) cannot convert to sendmessage service.' % (wsdl_url))

	if period_s <= 60:
		logging.critical('Period of sending data cant be less 60 secs')
		exit()
	elif period_s > 86400:
		logging.critical('Period of sending data cant be more then 1 day = 86400 secs')
		exit()

	GPIO.setmode(GPIO.BCM)
	GPIO.setwarnings(False)
	GPIO.setup(17, GPIO.OUT)
	GPIO.setup(18, GPIO.IN)				# Все зависит от ИК-порта --- , pull_up_down=GPIO.PUD_UP)	# Сразу взводим для исключения ложных срабатываний

	# Запустим поток мигания светодиодом
	tLEDflasher = threading.Thread(target=LEDflasher, args=(0.5, flash_led_period_s, ))
	tLEDflasher.daemon = True
	tLEDflasher.start()

	# Запустим поток обработки очереди сообщений
	qthr = threading.Thread(target=queue_thread_prc)
	qthr.daemon = True
	qthr.start()

	# Запустим поток отсылки данных
	sthr = threading.Thread(target=sender_thread, args=(period_s, ))
	sthr.daemon = True
	sthr.start()

	# Запустим поток определения наличия сети
	dthr = threading.Thread(target=detect_internet)
	dthr.daemon = True
	dthr.start()
except Exception as err:
	logging.critical(str(err))

	# Убьем поток мигалки индикатором
	if not tLEDflasher is None:
		kill_LEDflasher = True
		while fLEDflasher:
			sleep(0.5)

	# Убьем поток обработки сообщений
	if not qthr is None:
		if queue_thread:
			item = {'diff' : None, 'datetime' : None}
			PortQueue.put(item)
			while queue_thread:
				sleep(0.5)

	# Убьем поток отсылки данных
	if not sthr is None:
		if send_thread:
			send_event.set()
			while send_thread:
				sleep(0.5)

	# Убьем поток поиска сети
	if not dthr is None:
		if detect_thread:
			thread_error = True
			detect_event.set()
			while detect_thread:
				sleep(0.5)

	logging.critical("EXIT")
	exit()

# Основное тело скрипта --------------------------
try:
	sendSmsAndLOG("HELLO " + node_name)
	logging.info('HELLO ' + node_name)
	# основной цикл ожидания срабатывания сигнала
	currrise = datetime.datetime.now();
	total_time_without_signal_ms = 0
	while not thread_error:
		# IR диод мигает с частотой примерно < 10 мс
		signal = GPIO.wait_for_edge(18, GPIO.FALLING, timeout=IR_interrupt_ms)
		currfall = datetime.datetime.now();

		diffr_ms = int((currfall - currrise).total_seconds() * 1000)

		if signal is None:
			if total_time_without_signal_ms < closedetect_ms:
				total_time_without_signal_ms += IR_interrupt_ms
			elif LINE_EXISTS:
				close_detected()
			elif last_closedetect is None:
				close_detected()
			elif last_closedetect != currfall.date():
				close_detected()
			if LINE_EXISTS:
				line_state_tofile("0") #Пишем в файл изменение состояния линии
			LINE_EXISTS = False
		else:
			if (not LINE_EXISTS) and (not firsttime):
				line_normalized()
			if not LINE_EXISTS:
				line_state_tofile("1") #Пишем в файл изменение состояния линии
			LINE_EXISTS = True
			total_time_without_signal_ms = 0
			#check_diffr(diffr_ms, currfall)
			PortQueue.put({'diff' : diffr_ms, 'datetime' : currfall}, True)
			signal = GPIO.wait_for_edge(18, GPIO.RISING, timeout=IR_interrupt_ms)

		currrise = datetime.datetime.now();

		diffr_ms = int((currrise - currfall).total_seconds() * 1000)

		if signal is None:
			if total_time_without_signal_ms < closedetect_ms:
				total_time_without_signal_ms += IR_interrupt_ms
			elif LINE_EXISTS:
				close_detected()
			elif last_closedetect is None:
				close_detected()
			elif last_closedetect != currrise.date():
				close_detected()
			if LINE_EXISTS:
				line_state_tofile("0") #Пишем в файл изменение состояния линии
			LINE_EXISTS = False
		else:
			if (not LINE_EXISTS) and (not firsttime):
				line_normalized()
			if not LINE_EXISTS:
				line_state_tofile("1") #Пишем в файл изменение состояния линии
			LINE_EXISTS = True
			total_time_without_signal_ms = 0
			#check_diffr(diffr_ms, currrise)
			PortQueue.put({'diff' : diffr_ms, 'datetime' : currrise}, True)

		firsttime = False
		# Основное тело цикла

except Exception as e:
	sendSmsAndLOG('Counter error: ' + str(e))
	logging.critical(str(e))

sendSmsAndLOG('Счетчик больше не работает на узле <%s>' % (node_name))
logging.info("shutting down the programm")

# Убьем поток мигалки индикатором
logging.info("killing LED flasher thread")
if not tLEDflasher is None:
	kill_LEDflasher = True
	while fLEDflasher:
		sleep(0.5)

# Убьем поток обработки сообщений
logging.info("killing port queue thread")
if queue_thread:
	item = {'diff' : None, 'datetime' : None}
	PortQueue.put(item)
	while queue_thread:
		sleep(0.5)

logging.info("killing sender thread")
if send_thread:
	send_event.set()
	while send_thread:
		sleep(0.5)

logging.info("killing detect internet thread")
if detect_thread:
	thread_error = True
	detect_event.set()
	while detect_thread:
		sleep(0.5)

sleep(1)
logging.info('EXIT')

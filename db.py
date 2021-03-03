import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import datetime
import json
import numpy
import logging

logging.basicConfig(filename='publisher.log',
	format='%(levelname)s:%(asctime)s in %(module)s: %(message)s',
	level=logging.INFO)

connections = {}
engines = {}
sessions = {}

###################################################
def getEngine(db):
	sql = "SELECT 1"
	count = 0
	if db not in engines:
		connections[db] = 'mysql://{}:{}@{}/{}'.format(os.environ.get('{}_DB_USER'.format(db)),
												  os.environ.get('{}_DB_PASS'.format(db)),
												  os.environ.get('{}_DB_HOST'.format(db)),
												  os.environ.get('{}_DB_DATABASE'.format(db)))									  
		engines[db] = sqlalchemy.create_engine(connections[db], pool_recycle=3600, pool_pre_ping=True)
		Session = sessionmaker(bind=engines[db])
		sessions[db] = Session()

	while True:
		try:
			engines[db].execute(sql)
			break
		except Exception as e:
			logging.warning( 'getEngine - count {} {}'.format(count,e))
			count += 1
			if count == 2:
				return None
	return engines[db]

# SELECT task,dataset,status,count(*) as Count FROM `Activities` WHERE status = 'LAUNCHED' OR status = 'STARTED' GROUP BY task,dataset,status order by dataset,task,status 
# SELECT task,dataset,status,count(*) as Count FROM `Activities` GROUP BY task,dataset,status order by dataset,task,status 
###################################################
def db_launch(activity):
	engine = getEngine('operation')
	if engine is None: return None
	activity['status'] = 'LAUNCHED'
	activity['start'] = ''
	activity['launch'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	whereclause = ''
	value = ''
	for key in ['task','sceneid','dataset']:
		whereclause += key+'='
		if type(activity[key]) is str:
			value = "'{0}'".format(activity[key])
		else:
			value = "{0}".format(activity[key])
		whereclause += value+' AND '
	sql = "SELECT * FROM Activities WHERE {}".format(whereclause[:-5])
	logging.warning( 'db_launch - '+sql)
	result = engine.execute(sql)
	result = result.fetchone()
	sql = ''
	if result is None:
		params = ''
		values = ''
		for key in ['task','sceneid','dataset','launch','status']:
			params += key+','
			if type(activity[key]) is str:
				values += "'{0}',".format(activity[key])
			else:
				values += "{0},".format(activity[key])
			
		params += 'activity'
		values += "'{0}'".format(json.dumps(activity))
		sql = "INSERT INTO Activities ({0}) VALUES({1})".format(params,values)
	else:
		id = result['id']
		sql = "UPDATE Activities SET start=NULL,end=NULL,message='',launch='{}',elapsed=0,status='{}' WHERE id={}".format(activity['launch'],activity['status'],id)

	logging.warning( 'db_launch - '+sql)
	engine.execute(sql)
	return True

###################################################
def db_start(activity):
	engine = getEngine('operation')
	if engine is None: return None
	activity['status'] = 'STARTED'
	activity['start'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	whereclause = ''
	value = ''
	for key in ['task','sceneid','dataset']:
		whereclause += key+'='
		if type(activity[key]) is str:
			value = "'{0}'".format(activity[key])
		else:
			value = "{0}".format(activity[key])
		whereclause += value+' AND '
	sql = "UPDATE Activities SET start='{}',status='{}',activity='{}' WHERE {}".format(activity['start'],activity['status'],json.dumps(activity),whereclause[:-5])
	logging.warning( 'db_start - '+sql)
	engine.execute(sql)
	return True

###################################################
def db_end(activity):
	engine = getEngine('operation')
	if engine is None: return None
	activity['status'] = 'FINISHED'
	activity['end'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	if 'start' in activity and activity['start'] != '':
		start = numpy.datetime64(activity['start'])
		end = numpy.datetime64(activity['end'])
		activity['elapsed'] = int((end-start).astype(numpy.int16))
	else:
		activity['elapsed'] = 0
	whereclause = ''
	value = ''
	for key in ['task','sceneid','dataset']:
		whereclause += key+'='
		if type(activity[key]) is str:
			value = "'{0}'".format(activity[key])
		else:
			value = "{0}".format(activity[key])
		whereclause += value+' AND '
	sql= "UPDATE Activities SET end='{}',status='{}',elapsed={},message='{}' WHERE {}".format(activity['end'],activity['status'],activity['elapsed'],activity['message'],whereclause[:-5])
	logging.warning( 'db_end - '+sql)
	engine = getEngine('operation')
	engine.execute(sql)
	return True

###################################################
def db_error(activity):
	engine = getEngine('operation')
	if engine is None: return None
	activity['status'] = 'ERROR'
	activity['end'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	if 'start' in activity and activity['start'] != '':
		start = numpy.datetime64(activity['start'])
		end = numpy.datetime64(activity['end'])
		activity['elapsed'] = int((end-start).astype(numpy.int16))
	else:
		activity['elapsed'] = 0
	whereclause = ''
	value = ''
	for key in ['task','sceneid','dataset']:
		whereclause += key+'='
		if type(activity[key]) is str:
			value = "'{0}'".format(activity[key])
		else:
			value = "{0}".format(activity[key])
		whereclause += value+' AND '
	activity['message'] = activity['message'].replace('`','').replace('\'','')
	sql= "UPDATE Activities SET end='{}',status='{}',elapsed={},message='{}' WHERE {}".format(activity['end'],activity['status'],activity['elapsed'],activity['message'],whereclause[:-5])
	logging.warning( 'db_end - '+sql)
	engine = getEngine('operation')
	engine.execute(sql)
	return True

###################################################
def db_execute(sql,db='catalogo'):
	engine = getEngine(db)
	if engine is None: return None
	result = engine.execute(sql)
	return result

###################################################
def db_fetchone(sql,db='catalogo'):
	engine = getEngine(db)
	if engine is None: return None
	result = engine.execute(sql)
	result = result.fetchone()
	return result

###################################################
def db_fetchall(sql,db='catalogo'):
	engine = getEngine(db)
	if engine is None: return None
	result = engine.execute(sql)
	result = result.fetchall()
	return result

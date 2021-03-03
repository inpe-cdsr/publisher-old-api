"""
Describes the Celery Tasks definition of MS3 products
"""

# Python Native
import logging
import os
import time
from publisher.db import db_execute,db_fetchone,db_fetchall,db_launch,db_start,db_end,db_error
from datetime import datetime
import sqlalchemy
import time
import datetime
import json
import numpy
import scipy
from scipy import stats
from numpngw import write_png
import skimage
from skimage import exposure
from skimage.transform import resize
from skimage import morphology
from skimage.feature import register_translation,match_template
from skimage.transform import AffineTransform, SimilarityTransform,warp
from skimage.feature import corner_moravec,corner_fast,corner_harris,canny
from sklearn.ensemble import IsolationForest
from sklearn.metrics import r2_score
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
from osgeo.gdalconst import *
import MySQLdb
import zipfile
import zlib
import xmltodict
import copy
from celery import chain
import boto3
from publisher.celery import celery_app
import shutil
import math
import openpyxl

gdal.UseExceptions()
gdal.PushErrorHandler('CPLQuietErrorHandler')
logging.basicConfig(filename='publisher.log',
	format='%(levelname)s:%(asctime)s in %(module)s: %(message)s',
	level=logging.INFO)

###################################################
def loadConfig(sat):
	configfile = '{}.json'.format(sat)
	if os.path.exists(configfile):
		logging.warning(configfile+' was found in '+os.getcwd())
		with open(configfile) as infile:
			config = json.loads(infile.read())
		return config
	else:
		logging.warning(configfile+' was not found in '+os.getcwd())
		return None

###################################################
@celery_app.task(queue='processing')
def testgdal(activity):
	try:
#raise a GDAL error on purpose
		#gdal.Error(gdal.CE_Failure, 1, "Some error message")
		dataset = gdal.Open('xxx.tif',GA_ReadOnly)
	except Exception as e:
		logging.error('GetLastErrorMsg - {}'.format(gdal.GetLastErrorMsg()))
		logging.warning('testgdal {}'.format(e))
	return activity


###################################################
@celery_app.task(queue='processing')
def unzipScene(activity):
	activity['task'] = 'unzipScene'
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	activity['message'] = ''
	count = 0
	onecorrupt = False
	driver = gdal.GetDriverByName('GTiff')

# Create a new activity to publish the unzipped files
	newactivity = copy.deepcopy(activity)

# For each one of the files, unzip it in a temp directory, one level below the final directory
	for band in list(activity['files']):
		zfile = activity['files'][band]
		logging.warning('unzipScene - {}/{} - {}'.format(count+1,len(activity['files']),zfile))
		if count == 0:
			tifdirname = os.path.dirname(zfile).replace('Level-2','TIFF')
			newactivity['tiffdir'] = tifdirname
			tmpdirname = tifdirname+'/tmp'
			if not os.path.exists(tmpdirname):
				os.makedirs(tmpdirname)
		count += 1

# Check if file is not corrupt
		try:
			archive = zipfile.ZipFile(zfile, 'r')
			try:
				corrupt = True if archive.testzip() else False
			except :
				corrupt = True
				archive.close()
		except zipfile.BadZipfile:
			corrupt = True
		if corrupt:
			onecorrupt = True
			activity['status'] = 'ERROR'
			activity['message'] += 'Corrupt {} '.format(os.path.basename(zfile))
			logging.warning('unzipScene - {} is corrupt'.format(zfile))
			continue
		else:
			archive.extractall(tmpdirname)
			archive.close()

# Get the full name of unzipped file in temp directory and transform it into a internally zipped file in above directory
		tiffile = os.path.basename(zfile).replace('.zip','')
		src_filename = os.path.join(tmpdirname,tiffile)
		dst_filename = os.path.join(tifdirname,tiffile)
		newactivity['files'][band] = dst_filename
		logging.warning('unzipScene - creating {}'.format(dst_filename))
		try:
			src_ds = gdal.Open(src_filename)
			dst_ds = driver.CreateCopy(dst_filename, src_ds, options=["COMPRESS=LZW"])
			dst_ds = None
			src_ds = None
		except Exception as e:
			onecorrupt = True
			activity['status'] = 'ERROR'
			activity['message'] += gdal.GetLastErrorMsg()
			logging.error('unzipScene - GetLastErrorMsg - {}'.format(gdal.GetLastErrorMsg()))
			continue
# Copy the xml file from tmp to final directory
		xmlfile = src_filename.replace('.tif','.xml')
		if os.path.exists(xmlfile):
			shutil.copy2(xmlfile,tifdirname)
			newactivity['metadata'][band] = os.path.join(tifdirname,os.path.basename(xmlfile))

# Delete temp directory
	logging.warning('unzipScene - deleting {}'.format(tmpdirname))
	if os.path.exists(tmpdirname):
		shutil.rmtree(tmpdirname, ignore_errors=True)

# If there are corrupt files just return
	if onecorrupt:
		ret = db_error(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity
	else:
		ret = db_end(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
			return activity

# Publish the scene
	newactivity['task'] = 'generateQL'
	ret = db_launch(newactivity)
	if ret is None: 
		newactivity['status'] = 'ERROR'
		newactivity['message'] = 'Connection error'
		return newactivity
	newactivity['task'] = 'publishOneMS3'
	ret = db_launch(newactivity)
	if ret is None: 
		newactivity['status'] = 'ERROR'
		newactivity['message'] = 'Connection error'
		return newactivity
	taskschain = chain([generateQL.s(newactivity),publishOneMS3.s()])
	taskschain.apply_async()
	return activity

###################################################
@celery_app.task(queue='processing')
def registeringScene(activity):
	activity['task'] = 'registeringScene'
	activity['message'] = ''
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
# Get scenes to be registered
	scene1 = activity['scene1']
	scene2 = activity['scene2']
	
# Get files to be registered
	band = activity['band']
	file1 = scene1['files'][band]
	file2 = scene2['files'][band]
	
# Get rasters to be registered
	gdaldataset1 = gdal.Open(file1,GA_ReadOnly)
	geotransform1 = gdaldataset1.GetGeoTransform()
	gdaldataset2 = gdal.Open(file2,GA_ReadOnly)
	geotransform2 = gdaldataset2.GetGeoTransform()

	#raster1 = gdaldataset1.GetRasterBand(1).ReadAsArray()
	#raster2 = gdaldataset2.GetRasterBand(1).ReadAsArray()

	resolution = geotransform1[1]
	RasterXSize = min(gdaldataset1.RasterXSize,gdaldataset2.RasterXSize)
	RasterYSize = min(gdaldataset2.RasterYSize,gdaldataset2.RasterYSize)
	print('RasterXSize {} RasterYSize {}'.format(RasterXSize,RasterYSize))

	searchmargin = int(activity['searchmargin']/resolution)
	step = 200
	count = 0
	err_rows = []
	err_cols = []
	src_rows = []
	src_cols = []
	dst_rows = []
	dst_cols = []
	for y in range(searchmargin,RasterYSize-searchmargin,step):
		raster1 = gdaldataset1.GetRasterBand(1).ReadAsArray(0, y, RasterXSize, searchmargin)
		raster2 = gdaldataset2.GetRasterBand(1).ReadAsArray(0, y, RasterXSize, searchmargin)
		if activity['canny'] is not None:
			raster1 = 254*canny(raster1, sigma=2.5, low_threshold=0.2, high_threshold=0.8, mask=None, use_quantiles=True)
			raster2 = 254*canny(raster2, sigma=2.5, low_threshold=0.2, high_threshold=0.8, mask=None, use_quantiles=True)

		for x in range(searchmargin,RasterXSize-searchmargin,step):
			arr1 = raster1[:,x:x+searchmargin]
			if arr1.std() < 5.: continue
			arr2 = raster2[:,x:x+searchmargin]
			if arr2.std() < 5.: continue
			count += 1
			shift, error, diffphase = register_translation(arr1, arr2, upsample_factor=4)
			print('y {} x {} shift {} error {:.2f}'.format(y,x,shift, error))
			err_rows.append(shift[0])
			err_cols.append(shift[1])
			src_rows.append(y)
			src_cols.append(x)
			dst_rows.append(y-shift[0])
			dst_cols.append(x-shift[1])

# Detect outliers
	outliers = []
	fact = 3
	findOutliers(src_cols, resolution, err_rows, outliers, fact)
	logging.warning('1 - outliers {}'.format(outliers))
	findOutliers(src_cols, resolution, err_cols, outliers, fact)
	logging.warning('2 - outliers {}'.format(outliers))
	for i in range(len(src_cols)):
		if i in outliers:
			logging.warning('i {} col {} c {} a {} OUTLIER'.format(i,src_cols[i],round(err_rows[i],0),round(err_cols[i],0)))
		else:
			logging.warning('i {} col {} c {} a {}'.format(i,src_cols[i],round(err_rows[i],0),round(err_cols[i],0)))

# Filter the data removing the outliers
	acolumns = numpy.asarray(src_cols, dtype=numpy.float32)*resolution
	aoutliers = numpy.asarray(outliers, dtype=numpy.int)
	facolumns = numpy.delete(acolumns,aoutliers)
	aerr_rows = numpy.asarray(err_rows, dtype=numpy.float32)
	faerr_rows = numpy.delete(aerr_rows,aoutliers)
	aerr_cols = numpy.asarray(err_cols, dtype=numpy.float32)
	faerr_cols = numpy.delete(aerr_cols,aoutliers)
	err_cols_mean = faerr_cols.mean()

	errmedian_rows = round(numpy.median(faerr_rows),1)
	errmean_rows = round(numpy.mean(faerr_rows),1)
	errstd_rows = round(numpy.std(faerr_rows),1)

	errmedian_cols = round(numpy.median(faerr_cols),1)
	errmean_cols = round(numpy.mean(faerr_cols),1)
	errstd_cols = round(numpy.std(faerr_cols),1)

	logging.warning('errmean_rows {} errmean_cols {}'.format(round(errmean_rows,0),round(errmean_cols,0)))
	logging.warning('errmedian_rows {} errmedian_cols {}'.format(round(errmedian_rows,0),round(errmedian_cols,0)))
	logging.warning('errstd_rows {} errstd_cols {}'.format(round(errstd_rows,0),round(errstd_cols,0)))

	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity

###################################################
@celery_app.task(queue='processing')
def positioningScene(activity):
	activity['task'] = 'positioningScene'
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity

# Redefine satellite name according to grdb names
	sat = None
	if activity['sat'] == 'CBERS4A':
		sat = 'CBERS-4A'
	if activity['sat'] == 'CBERS4':
		sat = 'CBERS-4'
	if sat is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Not possible for {}'.format(activity['sat'])
		return activity

# Get compatibility for this at inst band
	bandnumber = None
	if activity['inst'] == 'MUX':
		bandmap = {'blue':5,'green':6,'red':7,'nir':8}
		bandnumber = bandmap[activity['band']]
	if activity['inst'] == 'WFI' or activity['inst'] == 'AWFI':
		bandmap = {'blue':13,'green':14,'red':15,'nir':16}
		bandnumber = bandmap[activity['band']]
	if activity['inst'] == 'WPM':
		bandmap = {'pan':0,'blue':1,'green':2,'red':3,'nir':4}
		bandnumber = bandmap[activity['band']]

	if bandnumber is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Not possible for {} {}'.format(activity['sat'],activity['inst'])
		return activity

# Define number of cols and lines in raw image
	resampling = activity['resampling']

	if activity['inst'] == 'WPM' and activity['band'] == 'pan':
		ncol = int(48000/resampling)
		nlin = int(48000/resampling)
	elif activity['inst'] == 'MUX':
		ncol = int(6000/resampling)
		nlin = int(6000/resampling)
	else:
		ncol = int(12000/resampling)
		nlin = int(12000/resampling)

	query = "SELECT COMPATIBLE_SATELLITE, COMPATIBLE_INSTRUMENT, COMPATIBLE_BAND FROM compatibility \
WHERE SATELLITE = '{}' AND INSTRUMENT = '{}' AND BAND = {} ORDER BY COMPATIBILITY_DEGREE ASC".format(sat,activity['inst'],bandnumber)
	logging.warning('positioningScene - query - {}'.format(query))
	compatlist = [('S2','MSI',8)]

	compatlist = []
	result = db_fetchall(query,'grdb')
	if result is None:
		activity['message'] = 'Connection error with grdb'
		ret = db_error(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity
	for res in result:
		logging.warning('positioningScene - compatelem - {}'.format(res))
		compatlist.append((res['COMPATIBLE_SATELLITE'],res['COMPATIBLE_INSTRUMENT'],res['COMPATIBLE_BAND']))

	logging.warning('positioningScene - compatlist - {}'.format(compatlist))

# Check if we have kernels for this scene, this will save processing time on scenes over sea
	file = activity['files'][activity['band']]
	logging.warning('positioningScene - file - {}'.format(file))
	gdaldataset = gdal.Open(file,GA_ReadOnly)
	geotransform = gdaldataset.GetGeoTransform()
	projection = gdaldataset.GetProjection()
	datasetsrs = osr.SpatialReference()
	datasetsrs.ImportFromWkt(projection)
	resolutionx = geotransform[1]
	resolutiony = geotransform[5]
	xmin = fllx = fulx = geotransform[0]
	ymax = fury = fuly = geotransform[3]
	xmax = furx = flrx = fulx + resolutionx * gdaldataset.RasterXSize
	ymin = flly = flry = fuly + resolutiony * gdaldataset.RasterYSize
	xcenter = (xmin+xmax)/2
	ycenter = (ymin+ymax)/2
# Create transformation from scene to ll coordinate
	llsrs = osr.SpatialReference()
	llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
	s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

# Evaluate corners coordinates in ll
#	Upper left corner
	(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
#	Upper right corner
	(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
#	Lower left corner
	(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
#	Lower right corner
	(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)
#	Center
	(longitude, latitude, nklrz ) = s2ll.TransformPoint( xcenter, ycenter)

	logging.warning( 'positioningScene - {} ullon {} ullat {} fulx {} fuly {}'.format(activity['sceneid'],ullon, ullat,fulx, fuly))
	grantotal = 0
	for compats in compatlist:
		satc = compats[0]
		instc = compats[1]
		bandc = compats[2]

		query = "SELECT * FROM kernel_primary WHERE SATELLITE = '{4}' AND INSTRUMENT = '{5}' AND BAND = {6} AND \
BOUNDING_BOX_UL_LAT_SAD69 <= {0} AND \
BOUNDING_BOX_UR_LAT_SAD69 <= {0} AND \
BOUNDING_BOX_LR_LAT_SAD69 >= {1} AND \
BOUNDING_BOX_LL_LAT_SAD69 >= {1} AND \
BOUNDING_BOX_LR_LON_SAD69 <= {2} AND \
BOUNDING_BOX_UR_LON_SAD69 <= {2} AND \
BOUNDING_BOX_UL_LON_SAD69 >= {3} AND \
BOUNDING_BOX_LL_LON_SAD69 >= {3} AND \
ACTIVE = 1 ORDER BY KERNEL_ID".format(ullat,lrlat,lrlon,ullon,satc,instc,bandc)
		result = db_fetchall(query,'grdb')
		if result is None:
			activity['message'] = 'Connection error with grdb'
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
		logging.warning('positioningScene - query - {} kernels {}'.format(query,len(result)))
# For each kernel
		grantotal += len(result)
	if grantotal == 0:
		activity['message'] = 'Scene {} has no kernels'.format(activity['sceneid'])
		ret = db_error(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity

# Estimate transform to map planar coordinates onto line column
	src = numpy.array([fulx,fuly,furx,fury,flrx,flry,fllx,flly]).reshape((4, 2))
	dst = numpy.array([0.,0.,ncol,0,ncol,nlin,0.,nlin]).reshape((4, 2))
	tform = AffineTransform()
	tform.estimate(src, dst)
	logging.warning('AffineTransform {}'.format(tform))
	print ('translation',tform.translation)
	print ('scale',tform.scale)
	print ('rotation',tform.rotation)
	print ('shear',tform.shear)
	test = tform(src[1])
	itest = tform.inverse(test)
	logging.warning('positioningScene - AffineTransform test {} -> {} -> {}'.format(src[1],test,itest))

# Correlation will be done on 8 bit enhanced images. Images may be resampled for efficiency purpose
	driver = gdal.GetDriverByName('GTiff')
# Resample image if it is required
	tifdirname = os.path.dirname(activity['files'][activity['band']].replace('TIFF','public'))
	logging.warning('positioningScene - checking tifdirname {}'.format(tifdirname))
	if activity['savekernels'] is not None and not os.path.exists(tifdirname):
		logging.warning('positioningScene - creating tifdirname {}'.format(tifdirname))
		os.makedirs(tifdirname)
# A resampled and enhanced image will be saved if requested by savekernels flag
	tifname = activity['files'][activity['band']].replace('.tif','_{}.tif'.format(resampling))
	tifname = tifname.replace('TIFF','public')
	tifdirname = os.path.dirname(tifname)
	if not os.path.exists(tifdirname):
		os.makedirs(tifdirname)
# If image is already resampled and enhanced, just read it
	if os.path.exists(tifname):
		try:
			gdaldataset = gdal.Open(tifname,GA_ReadOnly)
			geotransform = gdaldataset.GetGeoTransform()
			projection = gdaldataset.GetProjection()
			datasetsrs = osr.SpatialReference()
			datasetsrs.ImportFromWkt(projection)
			RasterXSize = gdaldataset.RasterXSize
			RasterYSize = gdaldataset.RasterYSize
			raster = gdaldataset.GetRasterBand(1).ReadAsArray(0, 0, RasterXSize, RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.warning('positioningScene - {}'.format(gdal.GetLastErrorMsg()))
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
	else:
# Get basic parameters from image file and the raster
		file = activity['files'][activity['band']]
		logging.warning('positioningScene - file - {}'.format(file))
		try:
			gdaldataset = gdal.Open(file,GA_ReadOnly)
			geotransform = gdaldataset.GetGeoTransform()
			projection = gdaldataset.GetProjection()
			datasetsrs = osr.SpatialReference()
			datasetsrs.ImportFromWkt(projection)
			RasterXSize = gdaldataset.RasterXSize
			RasterYSize = gdaldataset.RasterYSize
			raster = gdaldataset.GetRasterBand(1).ReadAsArray(0, 0, RasterXSize, RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.warning('positioningScene - {}'.format(gdal.GetLastErrorMsg()))
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
# Lets resample the image
		if resampling != 1:
			logging.warning('positioningScene - original geotransform {}'.format(geotransform))
			RasterYSize = int(gdaldataset.RasterYSize/resampling)
			RasterXSize = int(gdaldataset.RasterXSize/resampling)
			geotransform = [geotransform[0],geotransform[1]*resampling,geotransform[2],geotransform[3],geotransform[4],geotransform[5]*resampling]
			logging.warning('positioningScene - resampled geotransform {}'.format(geotransform))
# Due to memory limitations on some machines the resampling will be done not using resize
			if activity['inst'] == 'MUX' or activity['savememory'] is None:
				raster = resize(raster,(RasterYSize,RasterXSize), order=1, preserve_range=True)
			else:
				resampling2 = int(resampling/2)
				for l in range(RasterYSize):
					ll = l*resampling
					if l%2000 == 0:
						logging.warning('positioningScene - resampled line {}'.format(l))	
					cc = 0
					for c in range(RasterXSize):
						#raster[l,c] = raster[l*resampling+resampling2,c*resampling+resampling2]/4
						raster[l,c] = int(raster[ll:ll+resampling,cc:cc+resampling].mean())
						cc += resampling
				raster = raster[:RasterYSize,:RasterXSize]
# Enhance contrast and transform image to 8 bit
		a = numpy.array(raster.flatten())
		p1, p99 = numpy.percentile(a[a>0], (1, 99))
		raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(0,255)).astype(numpy.uint8)
# Save a copy of resampled tif file for later use if requested
		if activity['savekernels'] is not None and os.path.exists(tifdirname):
			try:
				tifdataset = driver.Create( tifname, RasterXSize, RasterYSize, 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
				tifdataset.SetGeoTransform(geotransform)
				tifdataset.SetProjection(projection)
				tifdataset.GetRasterBand(1).SetNoDataValue( 0 )
				tifdataset.GetRasterBand(1).WriteArray( raster )
				tifdataset = None
			except Exception as e:
				activity['message'] = 'Image manipulation error 1'
				logging.warning('positioningScene - {}'.format(activity['message']))
				ret = db_error(activity)
				if ret is None: 
					activity['status'] = 'ERROR'
					activity['message'] = 'Connection error'
				return activity
# Create and save thumbnail
	image = resize(raster,(600,600), order=1, preserve_range=True).astype(numpy.uint8)
	pngname = activity['files'][activity['band']].replace('.tif','.png')
	write_png(pngname, image, transparent=(0))

	method = 'DN'
	if activity['canny'] is not None:
		method = 'CANNY'
		raster = 254*canny(raster, sigma=2.5, low_threshold=0.2, high_threshold=0.8, mask=None, use_quantiles=True)
# Save a copy of canny tif file
		tifname = activity['files'][activity['band']].replace('.tif','_{}_{}.tif'.format(resampling,method))
		tifname = tifname.replace('TIFF','public')
		tifdirname = os.path.dirname(tifname)
		if not os.path.exists(tifdirname):
			os.makedirs(tifdirname)
		tifdataset = driver.Create( tifname, RasterXSize, RasterYSize, 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
# Set the geo-transform to the dataset
		tifdataset.SetGeoTransform(geotransform)
		tifdataset.SetProjection(projection)
		tifdataset.GetRasterBand(1).SetNoDataValue( 0 )
		tifdataset.GetRasterBand(1).WriteArray( raster )
		tifdataset = None

	resolutionx = geotransform[1]
	resolutiony = geotransform[5]

# Get some parameters from xml file
	xmlfile = activity['metadata'][activity['band']]
	if os.path.exists(xmlfile):
		with open(xmlfile) as fp:
			f = fp.readlines()
			fstring = ' '.join(f)
			xml = xmltodict.parse(fstring)
			if 'prdf' in xml:
				logging.warning('positioningScene - xmlfile - {}'.format(xmlfile))
				if 'viewing' in xml['prdf']:
					CenterTime = xml['prdf']['viewing']['center'].replace('T',' ')[:19]
					activity['CenterTime'] = CenterTime
				if 'orientationAngle' in xml['prdf']:
					orientationAngle = float(xml['prdf']['orientationAngle']['degree'])
					orientationAngle += float(xml['prdf']['orientationAngle']['minute'])/60.
					orientationAngle += float(xml['prdf']['orientationAngle']['second'])/3600.
					orientationAngle = numpy.radians(-orientationAngle)
					logging.warning( 'positioningScene - {} orientationAngle - {}'.format(activity['sceneid'],orientationAngle))
				if 'image' in xml['prdf']:
					processingTime = xml['prdf']['image']['processingTime'].replace('T',' ')[:19]
	else:
		activity['status'] = 'ERROR'
		activity['message'] = 'xmlfile {} not exist'.format(xmlfile)
		logging.warning('positioningScene - error - {}'.format(xmlfile))
		ret = db_error(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity
# Transform the search margin from meters to pixels
	searchmargin = int(activity['searchmargin']/min(resolutionx,-resolutiony))
	logging.warning('positioningScene - searchmargin - {}'.format(searchmargin))
	kernels = []
	columns = []
	lines = []
	err_alongs = []
	err_crosss = []
	err_ys = []
	err_xs = []
	k_keyval = {}
	k_keyval['sceneId'] = activity['sceneid'].split('-')[0]
	k_keyval['dataset'] = activity['dataset']
	k_keyval['band'] = activity['band']
	k_keyval['resampling'] = resampling
	k_keyval['method'] = method
# Delete registers if not first time
	params = 'WHERE '
	for key,val in k_keyval.items():
		params += key+'='
		if type(val) is str:
				params += "'{0}' AND ".format(val)
		else:
				params += "{0} AND ".format(val)

	sql = "DELETE FROM kernelpositioning {0}".format(params[:-5])
	logging.warning('positioningScene - deletek - {}'.format(sql))
	db_execute(sql,'operation')
	k_keyval['path'] = activity['path']
	k_keyval['row'] = activity['row']
	positioningdate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	k_keyval['positioningdate'] = positioningdate
	k_keyval['centertime'] = activity['CenterTime']
	k_keyval['processingdate'] = processingTime

# Create a raster to store originals GCPs in exact position
	rasterO = numpy.zeros(raster.shape, dtype=numpy.uint8)
# Create a raster to store GCPs in the position where they were correlated
	rasterT = numpy.zeros(raster.shape, dtype=numpy.uint8)

# Lets process the kernels
	success = 0
	total = 0
	grantotal = 0
	
	for compats in compatlist:
		satc = compats[0]
		instc = compats[1]
		bandc = compats[2]

		query = "SELECT * FROM kernel_primary WHERE SATELLITE = '{4}' AND INSTRUMENT = '{5}' AND BAND = {6} AND \
BOUNDING_BOX_UL_LAT_SAD69 <= {0} AND \
BOUNDING_BOX_UR_LAT_SAD69 <= {0} AND \
BOUNDING_BOX_LR_LAT_SAD69 >= {1} AND \
BOUNDING_BOX_LL_LAT_SAD69 >= {1} AND \
BOUNDING_BOX_LR_LON_SAD69 <= {2} AND \
BOUNDING_BOX_UR_LON_SAD69 <= {2} AND \
BOUNDING_BOX_UL_LON_SAD69 >= {3} AND \
BOUNDING_BOX_LL_LON_SAD69 >= {3} AND \
ACTIVE = 1 ORDER BY KERNEL_ID".format(ullat,lrlat,lrlon,ullon,satc,instc,bandc)
		result = db_fetchall(query,'grdb')
		if result is None:
			activity['message'] = 'Connection error with grdb'
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
		logging.warning('positioningScene - query - {} kernels {}'.format(query,len(result)))
# For each kernel
		grantotal += len(result)
		for row in result:
			#logging.warning('positioningScene - row - {} {}'.format(row['KERNEL_ID'],row[0]))
			k = Kernel(row)
			k.transformKernelBox(datasetsrs)

# Evaluate position of kernel in scene col/row coordinates
			kulcol = int((k.nkulx - fulx)/resolutionx)
			kulrow = int((k.nkuly - fuly)/resolutiony)
			klrcol = int((k.nklrx - fulx)/resolutionx)
			klrrow = int((k.nklry - fuly)/resolutiony)

			logging.warning( 'positioningScene - {} kulcol {} kulrow {} klrcol {} klrrow {}'.format(activity['sceneid'],kulcol, kulrow, klrcol, klrrow))
			if kulcol < 0:
				logging.warning( 'positioningScene - out {} KERNEL_ID {} kulcol {}'.format(activity['sceneid'],row['KERNEL_ID'],kulcol))
				continue
			if kulrow < 0: 
				logging.warning( 'positioningScene - out {} KERNEL_ID {} kulrow {}'.format(activity['sceneid'],row['KERNEL_ID'],kulrow))
				continue
			if klrcol >= RasterXSize:
				logging.warning( 'positioningScene - out {} KERNEL_ID {} klrcol {}'.format(activity['sceneid'],row['KERNEL_ID'],klrcol))
				continue
			if klrrow >= RasterYSize:
				logging.warning( 'positioningScene - out {} KERNEL_ID {} klrrow {}'.format(activity['sceneid'],row['KERNEL_ID'],klrrow))
				continue

# Evaluate search area in col row and check if kernel is inside useful scene area and correlation should be performed
			firstcol = max(kulcol - searchmargin,0)
			lastcol  = min(klrcol + searchmargin,RasterXSize)
			firstrow = max(kulrow - searchmargin,0)
			lastrow  = min(klrrow + searchmargin,RasterYSize)
			#logging.warning( 'positioningScene - {} firstcol {} firstrow {} lastcol {} lastrow {}'.format(activity['sceneid'],firstcol, firstrow, lastcol, lastrow))
# nodata area?
			smean = raster[firstrow:lastrow,firstcol:lastcol].mean()
			if activity['canny'] is None and smean <= 10:
				logging.warning( 'positioningScene - out {} KERNEL_ID {} smean {}'.format(activity['sceneid'],row['KERNEL_ID'],smean))
				continue
			if activity['canny'] is not None and smean <= 1:
				logging.warning( 'positioningScene - out {} KERNEL_ID {} smean {}'.format(activity['sceneid'],row['KERNEL_ID'],smean))
				continue

# Get the kernel blob in scene projection
			status = k.warpKernel(datasetsrs,resolutionx,resolutiony,activity)
# search area is smaller than kernel?
			if (lastrow - firstrow) < k.kernel.shape[0] or (lastcol - firstcol)  < k.kernel.shape[1]:
				logging.warning( 'positioningScene - {} KERNEL_ID {} search area is smaller than kernel'.format(activity['sceneid'],row['KERNEL_ID']))
				continue
# Evaluate position of kernel in scene col/row coordinates after warping and clipping
			kulcol = int((k.nkulx - fulx)/resolutionx)
			kulrow = int((k.nkuly - fuly)/resolutiony)
			klrcol = int((k.nklrx - fulx)/resolutionx)
			klrrow = int((k.nklry - fuly)/resolutiony)
			#logging.warning( 'positioningScene - {} kulcol {} kulrow {} klrcol {} klrrow {}'.format(activity['sceneid'],kulcol, kulrow, klrcol, klrrow))
# Save the original kernel 
			rasterO[kulrow:kulrow+k.kernel.shape[0],kulcol:kulcol+k.kernel.shape[1]] = k.kernel
# Revaluate search area in col row 
			firstcol = max(kulcol - searchmargin,0)
			lastcol  = min(klrcol + searchmargin,RasterXSize)
			firstrow = max(kulrow - searchmargin,0)
			lastrow  = min(klrrow + searchmargin,RasterYSize)

			result = match_template(raster[firstrow:lastrow,firstcol:lastcol], k.kernel)
			confidence = numpy.amin(result)
			#logging.warning( 'positioningScene - {} x {} y {}'.format(activity['sceneid'],raster[firstrow:lastrow,firstcol:lastcol].shape,k.kernel.shape))
			#shift, error, diffphase = register_translation(k.kernel, raster[firstrow:lastrow,firstcol:lastcol], upsample_factor=2)
			ij = numpy.unravel_index(numpy.argmax(result), result.shape)
# x,y is the position of upper left pixel of the search area where the correlation was a maximum
			x, y = ij[::-1]
			max_correl = result[y,x]
			logging.warning( 'positioningScene - {} kid {} x {} y {} max_correl {} confidence {} smean {}'.format(activity['sceneid'],row[0],x, y, max_correl,confidence,smean))
# ksx,ksy is the planar coordinate of upper left pixel of kernel where the maximum correlation was found
			ksx = fulx + resolutionx * (x + firstcol)
			ksy = fuly + resolutiony * (y + firstrow)
# Save the correlated kernel 
			rasterT[y + firstrow:y + firstrow+k.kernel.shape[0],x + firstcol:x + firstcol+k.kernel.shape[1]] = k.kernel
# x,y now is the position, in the full scene, of upper left corner of the kernel where the correlation was a maximum
			x += firstcol
			y += firstrow
# sx,sy is the planar coordinate of correlation point in the full scene
			sx = fulx + resolutionx * x
			sy = fuly + resolutiony * y
# kx,ky is the planar coordinate of upper left corner of kernel
			kx = k.nkulx
			ky = k.nkuly
			total += 1
			if max_correl >= activity['correlation']:
				success += 1
				kernels.append(row[0])
# Positioning error north oriented
				err_x = sx - kx
				err_y = sy - ky
				logging.warning( 'positioningScene - {} kid {} sx {} kx {} err_x {} sy {} ky {} err_y {}'.format(activity['sceneid'],row[0],sx, kx, err_x, sy, ky, err_y))
# Positioning error orbit oriented
				err_cross = err_x * math.cos(orientationAngle) + err_y * math.sin(orientationAngle)
				err_along = -err_x * math.sin(orientationAngle) + err_y * math.cos(orientationAngle)
# Position in instrument reference system
				collin = tform(numpy.array([sx,sy]))
				col = int(round(collin[0][0],0))
				lin = int(round(collin[0][1],0))
				columns.append(col)
				lines.append(lin)
				err_crosss.append(err_cross)
				err_alongs.append(err_along)
				err_xs.append(err_x)
				err_ys.append(err_y)
				logging.warning('positioningScene - collin {}'.format(collin))
				k_keyval['kernelid'] = row[0]
				k_keyval['correlation'] = max_correl
				k_keyval['kernel_x'] = kx
				k_keyval['kernel_y'] = ky
				k_keyval['scene_x'] = sx
				k_keyval['scene_y'] = sy
				k_keyval['err_x'] = err_x
				k_keyval['err_y'] = err_y
				k_keyval['longitude'] = (row[26]+row[27]+row[28]+row[29])/4
				k_keyval['latitude'] = (row[22]+row[23]+row[24]+row[25])/4
				k_keyval['col'] = col*resampling
				k_keyval['lin'] = lin*resampling
				k_keyval['err_cross'] = err_cross
				k_keyval['err_along'] = err_along
				params = ''
				values = ''
				for key,val in k_keyval.items():
					params += key+','
					if type(val) is str:
							values += "'{0}',".format(val)
					else:
							values += "{0},".format(val)
			
				sql = "INSERT INTO kernelpositioning ({0}) VALUES({1})".format(params[:-1],values[:-1])
				logging.warning('positioningScene - insertk - {}'.format(sql))
				db_execute(sql,'operation')
				tifname = '{}/GCP_{}_T.tif'.format(os.path.dirname(activity['files'][activity['band']]),row[0])
				logging.warning('positioningScene OK - {} err_x {} err_y {} max_correl {} smean {}'.format(os.path.basename(tifname),err_x,err_y,max_correl,smean))
				if False and activity['savekernels'] is not None and not os.path.exists(tifname):
					tifdataset = driver.Create( tifname, k.numcol, k.numrow, 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
# Set the geo-transform to the dataset
					#tifdataset.SetGeoTransform([nkulx, resolutionx, 0, nkuly, 0, resolutiony])
					tifdataset.SetGeoTransform([ksx, resolutionx, 0, ksy, 0, resolutiony])
					tifdataset.SetProjection(projection)
					tifdataset.GetRasterBand(1).WriteArray( k.kernel )
					tifdataset = None
		logging.error('positioningScene - grantotal {} total {} success {}'.format(grantotal,total,success))

# Save auxilliary GCPs files
	tifname = activity['files'][activity['band']].replace('.tif','_{}_{}_GCPO.tif'.format(resampling,method))
	tifname = tifname.replace('TIFF','public')
	tifdirname = os.path.dirname(tifname)
	if not os.path.exists(tifdirname):
		os.makedirs(tifdirname)
	if activity['savekernels'] is not None:
		tifdataset = driver.Create( tifname, rasterO.shape[1], rasterO.shape[0], 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
		tifdataset.SetGeoTransform(geotransform)
		tifdataset.SetProjection(projection)
		tifdataset.GetRasterBand(1).WriteArray( rasterO )
		tifdataset.GetRasterBand(1).SetNoDataValue( 0 )
		tifdataset = None

	tifname = activity['files'][activity['band']].replace('.tif','_{}_{}_GCPT.tif'.format(resampling,method))
	tifname = tifname.replace('TIFF','public')
	tifdirname = os.path.dirname(tifname)
	if not os.path.exists(tifdirname):
		os.makedirs(tifdirname)
	if activity['savekernels'] is not None:
		tifdataset = driver.Create( tifname, rasterT.shape[1], rasterT.shape[0], 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
		tifdataset.SetGeoTransform(geotransform)
		tifdataset.SetProjection(projection)
		tifdataset.GetRasterBand(1).WriteArray( rasterT )
		tifdataset.GetRasterBand(1).SetNoDataValue( 0 )
		tifdataset = None

# Create summary of scene positioning parameters to store in scenepositioning table
	s_keyval = {}
	s_keyval['sceneId'] = activity['sceneid'].split('-')[0]
	s_keyval['dataset'] = activity['dataset']
	s_keyval['path'] = activity['path']
	s_keyval['row'] = activity['row']
	s_keyval['xmin'] = xmin
	s_keyval['xmax'] = xmax
	s_keyval['ymin'] = ymin
	s_keyval['ymax'] = ymax
	s_keyval['latitude'] = latitude
	s_keyval['longitude'] = longitude
	s_keyval['band'] = activity['band']
	s_keyval['positioningdate'] = positioningdate
	s_keyval['centertime'] = activity['CenterTime']
	s_keyval['processingdate'] = processingTime
	s_keyval['correlation'] = activity['correlation']
	s_keyval['resampling'] = resampling
	s_keyval['method'] = method
	s_keyval['thumbnail'] = pngname

# Enough kernels?
	if len(kernels) < 2:
		s_keyval['kernels'] = len(kernels)
		
		s_keyval['crossslope'] = 0
		s_keyval['crossbias'] = 0
		s_keyval['crossr2'] = 0
		s_keyval['err_cross'] = 0
		s_keyval['err_cross_mean'] = 0 if len(kernels) == 0 else err_crosss[0]
		s_keyval['err_cross_std'] = 0

		s_keyval['alongslope'] = 0
		s_keyval['alongbias'] = 0
		s_keyval['alongr2'] = 0
		s_keyval['err_along'] = 0
		s_keyval['err_along_mean'] = 0 if len(kernels) == 0 else err_alongs[0]
		s_keyval['err_along_std'] = 0

		s_keyval['xslope'] = 0
		s_keyval['xbias'] = 0
		s_keyval['xr2'] = 0
		s_keyval['err_x'] = 0
		s_keyval['err_x_mean'] = 0 if len(kernels) == 0 else err_xs[0]
		s_keyval['err_x_std'] = 0

		s_keyval['yslope'] = 0
		s_keyval['ybias'] = 0
		s_keyval['yr2'] = 0
		s_keyval['err_y'] = 0
		s_keyval['err_y_mean'] = 0 if len(kernels) == 0 else err_ys[0]
		s_keyval['err_y_std'] = 0

		params = ''
		values = ''
		for key,val in s_keyval.items():
			params += key+','
			if type(val) is str:
					values += "'{0}',".format(val)
			else:
					values += "{0},".format(val)

		sql = "DELETE FROM scenepositioning WHERE sceneId='{}' AND dataset='{}' AND resampling={} AND method='{}' AND band='{}'".format(s_keyval['sceneId'],s_keyval['dataset'],s_keyval['resampling'],s_keyval['method'],s_keyval['band'])
		logging.warning('positioningScene - deletes - {}'.format(sql))
		db_execute(sql,'operation')
		sql = "INSERT INTO scenepositioning ({0}) VALUES({1})".format(params[:-1],values[:-1])
		logging.warning('positioningScene - inserts - {}'.format(sql))
		db_execute(sql,'operation')

		activity['message'] = 'Scene {} has only {}/{} kernels'.format(activity['sceneid'],len(kernels),grantotal)
		ret = db_error(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity

# Detect outliers
	outliers = []
	fact = activity['sigma']
	findOutliers(columns, resolutionx, err_crosss, outliers, fact)
	logging.warning('1 - outliers {}'.format(outliers))
	findOutliers(columns, resolutionx, err_alongs, outliers, fact)
	logging.warning('2 - outliers {}'.format(outliers))
	for i in range(len(columns)):
		if i in outliers:
			logging.warning('i {} col {} c {} a {} OUTLIER'.format(i,columns[i],round(err_crosss[i],0),round(err_alongs[i],0)))
		else:
			logging.warning('i {} col {} c {} a {}'.format(i,columns[i],round(err_crosss[i],0),round(err_alongs[i],0)))

# Delete outliers from kernelpositioning
	for outlier in outliers:
		kernelid = kernels[outlier]
		sql = "UPDATE kernelpositioning SET status='OUTLIER' WHERE sceneId='{}' AND dataset='{}' AND kernelid={} AND resampling={} AND method='{}'".format(s_keyval['sceneId'],s_keyval['dataset'],kernelid,s_keyval['resampling'],s_keyval['method'])
		logging.warning('sql - {}'.format(sql))
		db_execute(sql,'operation')

# Filter the data removing the outliers
	acolumns = numpy.asarray(columns, dtype=numpy.float32)*resolutionx*resampling
	aoutliers = numpy.asarray(outliers, dtype=numpy.int)
	facolumns = numpy.delete(acolumns,aoutliers)
	s_keyval['kernels'] = facolumns.shape[0]

# Evaluate linear regression col x err_cross
	aerr_crosss = numpy.asarray(err_crosss, dtype=numpy.float32)
	faerr_crosss = numpy.delete(aerr_crosss,aoutliers)
	slope, bias, r_value, p_value, std_err = stats.linregress(facolumns,faerr_crosss)
	r2 = r2_score(faerr_crosss, facolumns * slope + bias)
	logging.warning('linregress err_crosss - slope {} bias {} r_value {} p_value {} std_err {} r2 {}'.format(slope, bias, r_value, p_value, std_err,r2))
	s_keyval['crossslope'] = slope
	s_keyval['crossbias'] = bias
	s_keyval['crossr2'] = r2
	err1 = 1*slope + bias
	err2 = ncol*slope*resolutionx + bias
	errmean = round((err1+err2)/2.,1)
# Find nearest to zero in numpy array
	errmin = round(min(faerr_crosss, key=abs),1)
	errmedian = round(numpy.median(faerr_crosss),1)
	errmean = round(numpy.mean(faerr_crosss),1)
	errstd = round(numpy.std(faerr_crosss),1)
	logging.warning('faerr_crosss {} - min {} mean {} std {}'.format(faerr_crosss, errmin, errmean, errstd))
	s_keyval['err_cross'] = errmin
	s_keyval['err_cross_mean'] = errmean
	s_keyval['err_cross_std'] = errstd

# Evaluate linear regression col x err_along
	aerr_alongs = numpy.asarray(err_alongs, dtype=numpy.float32)
	faerr_alongs = numpy.delete(aerr_alongs,aoutliers)
	slope, bias, r_value, p_value, std_err = stats.linregress(facolumns,faerr_alongs)
	r2 = r2_score(faerr_alongs, facolumns * slope + bias)
	logging.warning('linregress err_along - slope {} bias {} r_value {} p_value {} std_err {} r2 {}'.format(slope, bias, r_value, p_value, std_err,r2))
	s_keyval['alongslope'] = slope
	s_keyval['alongbias'] = bias
	s_keyval['alongr2'] = r2
	err1 = 1*slope + bias
	err2 = ncol*slope*resolutionx + bias
	errmean = int((err1+err2)/2.)
	errmin = round(min(faerr_alongs, key=abs),1)
	errmedian = round(numpy.median(faerr_alongs),1)
	errmean = round(numpy.mean(faerr_alongs),1)
	errstd = round(numpy.std(faerr_alongs),1)
	logging.warning('faerr_alongs {} - min {} mean {} std {}'.format(faerr_alongs, errmin, errmean, errstd))
	s_keyval['err_along'] = errmin
	s_keyval['err_along_mean'] = errmean
	s_keyval['err_along_std'] = errstd

# Evaluate linear regression col x err_x
	aerr_xs = numpy.asarray(err_xs, dtype=numpy.float32)
	faerr_xs = numpy.delete(aerr_xs,aoutliers)
	slope, bias, r_value, p_value, std_err = stats.linregress(facolumns,faerr_xs)
	r2 = r2_score(faerr_xs, facolumns * slope + bias)
	logging.warning('linregress err_x - slope {} bias {} r_value {} p_value {} std_err {} r2 {}'.format(slope, bias, r_value, p_value, std_err,r2))
	s_keyval['xslope'] = slope
	s_keyval['xbias'] = bias
	s_keyval['xr2'] = r2
	err1 = 1*slope + bias
	err2 = ncol*slope*resolutionx + bias
	errmean = int((err1+err2)/2.)
	errmin = round(min(faerr_xs, key=abs),1)
	errmedian = round(numpy.median(faerr_xs),1)
	errmean = round(numpy.mean(faerr_xs),1)
	errstd = round(numpy.std(faerr_xs),1)
	logging.warning('faerr_xs {} - min {} mean {} std {}'.format(faerr_xs, errmin, errmean, errstd))
	s_keyval['err_x'] = errmin
	s_keyval['err_x_mean'] = errmean
	s_keyval['err_x_std'] = errstd

# Evaluate linear regression col x err_y
	aerr_ys = numpy.asarray(err_ys, dtype=numpy.float32)
	faerr_ys = numpy.delete(aerr_ys,aoutliers)
	slope, bias, r_value, p_value, std_err = stats.linregress(facolumns,faerr_ys)
	r2 = r2_score(faerr_ys, facolumns * slope + bias)
	logging.warning('linregress err_y - slope {} bias {} r_value {} p_value {} std_err {} r2 {}'.format(slope, bias, r_value, p_value, std_err,r2))
	s_keyval['yslope'] = slope
	s_keyval['ybias'] = bias
	s_keyval['yr2'] = r2
	err1 = 1*slope + bias
	err2 = ncol*slope*resolutionx + bias
	errmean = int((err1+err2)/2.)
	errmin = round(min(faerr_ys, key=abs),1)
	errmedian = round(numpy.median(faerr_ys),1)
	errmean = round(numpy.mean(faerr_ys),1)
	errstd = round(numpy.std(faerr_ys),1)
	logging.warning('faerr_ys {} - min {} mean {} std {}'.format(faerr_ys, errmin, errmean, errstd))
	s_keyval['err_y'] = errmin
	s_keyval['err_y_mean'] = errmean
	s_keyval['err_y_std'] = errstd

	params = ''
	values = ''
	for key,val in s_keyval.items():
		params += key+','
		if type(val) is str:
				values += "'{0}',".format(val)
		else:
				values += "{0},".format(val)

	sql = "DELETE FROM scenepositioning WHERE sceneId='{}' AND dataset='{}' AND resampling={} AND method='{}' AND band='{}'".format(s_keyval['sceneId'],s_keyval['dataset'],s_keyval['resampling'],s_keyval['method'],s_keyval['band'])
	logging.warning('positioningScene - deletes - {}'.format(sql))
	db_execute(sql,'operation')
	sql = "INSERT INTO scenepositioning ({0}) VALUES({1})".format(params[:-1],values[:-1])
	logging.warning('positioningScene - inserts - {}'.format(sql))
	db_execute(sql,'operation')

# Save a copy of tif file with corrected boundind box
	tifname = activity['files'][activity['band']].replace('.tif','_{}_{}_C.tif'.format(resampling,method))
	tifname = tifname.replace('TIFF','public')
	tifdirname = os.path.dirname(tifname)
	if not os.path.exists(tifdirname):
		os.makedirs(tifdirname)
	tifdataset = driver.Create( tifname, RasterXSize, RasterYSize, 1, gdal.GDT_Byte , options = ['COMPRESS=LZW'])
# Set the geo-transform to the dataset
	tifdataset.SetGeoTransform([geotransform[0]-s_keyval['err_x'], resolutionx, 0, geotransform[3]-s_keyval['err_y'], 0, resolutiony])
	tifdataset.SetProjection(projection)
	tifdataset.GetRasterBand(1).SetNoDataValue( 0 )
	tifdataset.GetRasterBand(1).WriteArray( raster )
	tifdataset = None

	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity

########################
def findOutliers(columns, resolutionx, errors, outliers, fact = 3):
# Evaluate linear regression columns*resolutionx vs errors
	acolumns = numpy.asarray(columns, dtype=numpy.float32)*resolutionx
	aerrors = numpy.asarray(errors, dtype=numpy.float32)
	slope, bias, r_value, p_value, std_err = stats.linregress(acolumns,aerrors)
	r2 = r2_score(aerrors, acolumns * slope + bias)
	logging.warning('errors - min {} mean {} max {} std {}'.format(aerrors.min(), aerrors.mean(), aerrors.max(), aerrors.std()))
	logging.warning('linregress errors - slope {} bias {} r_value {} p_value {} std_err {} r2 {}'.format(slope, bias, r_value, p_value, std_err,r2))

# Detect outliers columns x errors using the IQR (interquartile range) method 
	lower_range,upper_range = outlier_treatment(errors)
	outliersIQR = numpy.where((aerrors < lower_range) | (aerrors > upper_range))
	logging.warning('outlier_treatment - columns x errors - lower_range {} upper_range {} {}'.format(lower_range,upper_range,outliersIQR[0]))
	for outlier in outliersIQR[0]:
		if outlier not in outliers:
			outliers.append(outlier)

# Detect outliers columns x errors using the n sigma method
	lower_range = aerrors.mean() - fact*aerrors.std()
	upper_range = aerrors.mean() + fact*aerrors.std()
	outliersS = numpy.where((aerrors < lower_range) | (aerrors > upper_range))
	logging.warning('sigma - columns x errors - lower_range {} upper_range {} {}'.format(lower_range,upper_range,outliersS[0]))
	for outlier in outliersS[0]:
		if outlier not in outliers:
			outliers.append(outlier)

# Detect outliers using the IsolationForest method
	X = (columns,errors)
	X = numpy.asarray(X).T
	clf = IsolationForest()
	predict = clf.fit_predict(X)
	score = clf.score_samples(X)
	#outliers = numpy.where((predict == -1) & (score < -0.70))
	outliersIF = numpy.where(predict == -1)
	logging.warning('IsolationForest outliers {}'.format(outliersIF[0]))
	for i in range(len(columns)):
		logging.warning('i {} p {} s {} c {} e {}'.format(i,predict[i],round(score[i],2),X[i,0],int(X[i,1])))
	for outlier in outliersIF[0]:
		if outlier not in outliers:
			outliers.append(outlier)

########################
def outlier_treatment(errors):
	Q1,Q3 = numpy.percentile(sorted(errors) , [25,75])
	IQR = Q3 - Q1
	lower_range = Q1 - (1.5 * IQR)
	upper_range = Q3 + (1.5 * IQR)
	logging.warning('outlier_treatment errors {} Q1 {} Q3 {} IQR {} lower_range {} upper_range {}'.format(sorted(errors),Q1,Q3,IQR,lower_range,upper_range))
	return lower_range,upper_range

########################
def distance(m,b,x,y):
	d = abs(b + m*x - y) / math.sqrt(1 + m*m)
	return d
	
########################
def linreg(x,y):
# Compute the slope (m)
	m = (len(x) * numpy.sum(x*y) - numpy.sum(x) * numpy.sum(y)) / (len(x)*numpy.sum(x*x) - numpy.sum(x) ** 2)
# Compute the bias (b)
	b = (numpy.sum(y) - m *numpy.sum(x)) / len(x)
	return (b,m)

########################
class Kernel:
	def __init__(self,row):
		self.row = row
		hemis = ""
		if float(row[22]) <= 0.:
			hemis = "+south"
		ol = row[15]
		zone = int((ol + 183) / 6)
		kernelproj4 = "+proj={} +zone={} +x_0={} +y_0={} {} +datum={} +units=m +no_defs".format(row[9].lower(),zone,row[12],row[13],hemis,row[10])
		#logging.warning('Kernel - kernelproj4 = {}'.format(kernelproj4))
		self.kernelsrs = osr.SpatialReference()
		self.kernelsrs.ImportFromProj4(kernelproj4)
# Evaluate Bounding Box for kernel
		self.kresx = row[18]
		self.kresy = row[19]
		self.numrow = row[5]
		self.numcol = row[6]
		self.kury = self.kuly = row[20] - float(row[13]) + 10000000
		self.kllx = self.kulx = row[21] + float(row[12]) - 500000
		self.kurx = self.klrx = self.kulx + self.kresx*self.numcol
		self.klly = self.klry = self.kuly - self.kresy*self.numrow
		#logging.warning('transformKernelBox - Kernel UL = ({},{})'.format(kulx,kuly))
		#logging.warning('transformKernelBox - Kernel LR = ({},{})'.format(klrx,klry))

########################
	def transformKernelBox(self,srs):
# Create transformation to scene reference system
		k2s = osr.CoordinateTransformation ( self.kernelsrs, srs )
# Evaluate corners coordinates in scene reference system
#	Upper left corner
		(self.nkulx, self.nkuly, nkulz ) = k2s.TransformPoint( self.kulx, self.kuly)
#	Lower right corner
		(self.nklrx, self.nklry, nklrz ) = k2s.TransformPoint( self.klrx, self.klry)
		#logging.warning('transformKernelBox - nKernel UL = ({},{})'.format(nkulx,nkuly))
		#logging.warning('transformKernelBox - nKernel LR = ({},{})'.format(nklrx,nklry))
		return

########################
	def warpKernel(self,srs,resolutionx,resolutiony,activity):
# Get the kernel blob
		query = "SELECT IMAGE FROM kernel_blob WHERE KERNEL_ID = '{}'".format(self.row[0])
		data = db_fetchone(query,'grdb')
		dbkernel = numpy.frombuffer(data[0], dtype=numpy.uint8)
		kernel = dbkernel.reshape((self.numrow,self.numcol))
		#logging.warning('warpKernel - kernel = {})'.format(kernel.shape))
# Create an in-memory raster for original kernel
		mem_drv = gdal.GetDriverByName( 'MEM' )
		src_ds = mem_drv.Create('', self.numcol, self.numrow, 1, gdal.GDT_Byte)
# Set the geotransform
		src_ds.SetGeoTransform([self.kulx, self.kresx, 0, self.kuly, 0, -self.kresy])
		src_ds.SetProjection (self.kernelsrs.ExportToWkt())
		src_ds.GetRasterBand(1).WriteArray( kernel )
		tifname = '{}/GCP_{}_O.tif'.format(os.path.dirname(activity['files'][activity['band']]),self.row[0])
		tifname = tifname.replace('TIFF','public')
		if activity['savekernels'] is not None and not os.path.exists(tifname):
			tdriver = gdal.GetDriverByName('GTiff')
			tifdataset = tdriver.CreateCopy(tifname, src_ds, options=["COMPRESS=LZW"])
			tifdataset = None
# Create an in-memory raster for warped kernel
		factorx = self.kresx/resolutionx
		factory = self.kresy/abs(resolutiony)
		numcol = int(round(kernel.shape[1]*factorx,0))
		numrow = int(round(kernel.shape[0]*factory,0))
		dst_ds = mem_drv.Create('', numcol, numrow, 1, gdal.GDT_Byte)
# Set the geotransform
		dst_ds.SetGeoTransform([self.nkulx, resolutionx, 0, self.nkuly, 0, resolutiony])
		dst_ds.SetProjection (srs.ExportToWkt())
		dst_ds.GetRasterBand(1).SetNoDataValue(0)

# Perform the warp/resampling
		""" 
if interpolation == 'bilinear': gdal_interp = gdal.GRA_Bilinear
elif interpolation == 'nearest': gdal_interp = gdal.GRA_NearestNeighbour
elif interpolation == 'lanczos': gdal_interp = gdal.GRA_Lanczos
elif interpolation == 'convolution': gdal_interp = gdal.GRA_Cubic # cubic convolution
elif interpolation == 'cubicspline': gdal_interp = gdal.GRA_CubicSpline # cubic spline
else: print(('Unknown interpolation method: '+interpolation))
		"""
		resampling = gdal.GRA_Bilinear
		try:
			res = gdal.ReprojectImage( src_ds, dst_ds, src_ds.GetProjection(), dst_ds.GetProjection(), resampling)
		except:
			return False
		tifname = tifname.replace('_O.tif','_T.tif')
		if activity['savekernels'] is not None and not os.path.exists(tifname):
			tdriver = gdal.GetDriverByName('GTiff')
			tifdataset = tdriver.CreateCopy(tifname, dst_ds, options=["COMPRESS=LZW"])
			tifdataset = None
		kwarped = dst_ds.GetRasterBand(1).ReadAsArray()
		logging.warning('warpKernel - kwarped = {})'.format(kwarped.shape))
# Clip the kernel to remove the zeroes caused by warping	
		zeros =  numpy.where(kwarped == 0)
		#logging.warning('warpKernel - zeros = {})'.format(zeros))
		#logging.warning('warpKernel - zeros = {})'.format(len(zeros[0])))
		flag = len(zeros[0]) > 0
		windowmargin = 0
		while flag:
			windowmargin += 1
			zeros =  numpy.where(kwarped[windowmargin:-windowmargin-1,windowmargin:-windowmargin-1] == 0)
			#logging.warning('warpKernel - windowmargin {} zeros = {})'.format(windowmargin,len(zeros[0])))
			lz = len(zeros[0])
			if lz == 0:
				flag = False
		
		self.kernel = kwarped[windowmargin:-windowmargin-1,windowmargin:-windowmargin-1]
		if activity['canny'] is not None:
			self.kernel = 254*canny(self.kernel, sigma=2.5, low_threshold=0.2, high_threshold=0.8, mask=None, use_quantiles=True).astype(numpy.uint8)
		self.numrow = self.kernel.shape[0]
		self.numcol = self.kernel.shape[1]
		self.nkulx = self.nkulx + windowmargin*resolutionx
		self.nkuly = self.nkuly + windowmargin*resolutiony
		self.nklrx = self.nklrx - windowmargin*resolutionx
		self.nklry = self.nklry - windowmargin*resolutiony
		#logging.warning('warpKernel - self.kernel = {})'.format(self.kernel.shape))

		return True

###################################################
@celery_app.task(queue='upload')
def uploadScene(activity):
	global S3Client,bucket_name
	activity['task'] = 'uploadScene'
	if activity['status'] == 'ERROR':
		activity['message'] = 'Previous error'
		db_error(activity)
		return 	activity
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity

### NOT DOING YET
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity

	S3Client = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
	bucket_name = os.environ.get('BUCKET_NAME')
	prefix = activity['tiffdir'].replace('/TIFF/','') + '/'
	logging.warning('uploadScene S3 prefix {} '.format(prefix))
	s3tiffs = []
	result = S3Client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
	if 'Contents' in result:
		for obj in result['Contents']:
			logging.warning('uploadScene S3 tiff {} '.format(obj.get('Key')))
			s3tiffs.append(os.path.basename(obj.get('Key')))
	count = 0
	for band in list(activity['files']):
		tiff = activity['files'][band]
		count += 1
		logging.warning('uploadScene {}/{} - {}'.format(count,len(activity['files']),tiff))
		if os.path.basename(tiff) in s3tiffs:
			logging.warning('uploadScene {} already in S3'.format(os.path.basename(tiff)))
			continue
		mykey = tiff.replace('/TIFF/','')
		
		try:
			tc = boto3.s3.transfer.TransferConfig()
			t = boto3.s3.transfer.S3Transfer( client=S3Client, config=tc )
			t.upload_file( tiff, bucket_name, mykey, extra_args={'ACL': 'public-read'})
		except Exception as e:
			logging.warning('uploadScene error {}'.format(e))
			activity['message'] = '{}'.format(e)
			db_error(activity)
			return activity
	png = activity['pngname']
	mykey = png.replace('/TIFF/','')
	try:
		tc = boto3.s3.transfer.TransferConfig()
		t = boto3.s3.transfer.S3Transfer( client=S3Client, config=tc )
		t.upload_file( png, bucket_name, mykey, extra_args={'ACL': 'public-read'})
	except Exception as e:
		logging.warning('uploadScene error {}'.format(e))
		activity['message'] = '{}'.format(e)
		db_error(activity)
		return activity
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity


###################################################
@celery_app.task(queue='quicklook')
def generateQL(activity):
	activity['task'] = 'generateQL'
	if activity['status'] == 'ERROR':
		activity['message'] = 'Previous error'
		db_error(activity)
		return 	activity
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	config = loadConfig(activity['sat'])
	print('generateQL',activity)

# Put files in an ordered list (red,green,blue)
	qlfiles = []
	for gband in config['quicklook'][activity['inst']]:
		#logging.warning('generateQL - band {} file {}'.format(gband,activity['files'][gband]))
		qlfiles.append(activity['files'][gband])
	gband = config['quicklook'][activity['inst']][0]
	template = '_'+config['publish'][activity['rp']][activity['inst']][gband]
	basename = os.path.basename(qlfiles[0]).replace(template,'')
	parts = basename.split('_')
	del parts[6]
	basename = '_'.join(parts)
	pngname = activity['tiffdir']+'/{}.png'.format(basename)
	activity['pngname'] = pngname
	if os.path.exists(pngname): 
		logging.warning('generateQL - {} png exists {}'.format(activity['sceneid'],pngname))
		activity['message'] = 'Already done'
		db_end(activity)
		return activity
	logging.warning('generateQL - {} png creating {}'.format(activity['sceneid'],pngname))
	dataset = gdal.Open(qlfiles[0],GA_ReadOnly)
	numlin = 768
	numcol = int(float(dataset.RasterXSize)/float(dataset.RasterYSize)*numlin)
	image = numpy.ones((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
	#logging.warning('generateQL - Y {} X {} {}'.format(dataset.RasterYSize,dataset.RasterXSize,image.shape))
	nb = 0
	for file in qlfiles:
		gband = config['quicklook'][activity['inst']][nb]
		logging.warning('generateQL - {} reading {}'.format(activity['sceneid'],os.path.basename(file)))
		try:
			dataset = gdal.Open(file,GA_ReadOnly)
			raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.error('generateQL - {}'.format(gdal.GetLastErrorMsg()))
			image = None
			db_error(activity)
			return activity
		raster = resize(raster,(numlin,numcol), order=1, preserve_range=True)
# Some images may have no valid pixels and will cause error on numpy.percentile
		if raster.min() == raster.max():
			logging.error('generateQL - No valid pixels in {}'.format(file))
			activity['message'] = 'No valid pixels in {}'.format(file)
			image = None
			db_error(activity)
			return activity
# Evaluate nodata mask
		nodata = raster <= 0
# Evaluate minimum and maximum values
		if 'limits' in activity and gband in activity['limits']:
			p1 = activity['limits'][gband]['p1']
			p99 = activity['limits'][gband]['p99']
		else:
			a = numpy.array(raster.flatten())
			#p1, p99 = numpy.percentile(a[a>0], (1, 99))
			p1, p99 = numpy.percentile(a[numpy.logical_and(a>0, a<1023)], (1, 99))
# Convert minimum and maximum values to 1,255 - 0 is nodata
		raster = exposure.rescale_intensity(raster, in_range=(p1, p99),out_range=(1,255))
		#logging.warning('generateQL -rescale_intensity - p1 {} p99 {} min {} max {}'.format(p1,p99,raster.min(),raster.max()))
		image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
		#logging.warning('generateQL -rescale_intensity - p1 {} p99 {} min {} max {}'.format(p1,p99,image[:,:,nb].min(),image[:,:,nb].max()))
		nb += 1
	#logging.warning('generateQL - pngname {} min {} max {} shape {}'.format(os.path.basename(pngname),image.min(),image.max(),image.shape))
	if nb == 1:
		write_png(pngname, image, transparent=(0))
	else:
		write_png(pngname, image, transparent=(0, 0, 0))
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity

###################################################
def evaluateLimits(activity,config):
	print('evaluateLimits',activity)

# Put files in an ordered list (red,green,blue)
	activity['limits'] = {}
	for gband in config['quicklook'][activity['inst']]:
		file = activity['files'][gband]
		logging.warning('evaluateLimits - {} reading {}'.format(activity['sceneid'],os.path.basename(file)))
		activity['limits'][gband] = {'p1':0,'p99':255}
		try:
			dataset = gdal.Open(file,GA_ReadOnly)
			raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.error('generateQL - {}'.format(gdal.GetLastErrorMsg()))
			raster = None
			return False
# Some images may have no valid pixels and will cause error on numpy.percentile
		if raster.min() == raster.max():
			logging.error('generateQL - No valid pixels in {}'.format(file))
			activity['message'] = 'No valid pixels in {}'.format(file)
			raster = None
			return False
# Evaluate nodata mask
		nodata = raster <= 0
# Evaluate minimum and maximum values
		a = numpy.array(raster.flatten())
		
		#p1, p99 = numpy.percentile(a[a>0], (1, 99))
		p1, p99 = numpy.percentile(a[numpy.logical_and(a>0, a<1023)], (1, 99))
		activity['limits'][gband] = {'p1':p1,'p99':p99}
		logging.warning('evaluateLimits - percentile - p1 {} p99 {} min {} max {}'.format(p1,p99,raster.min(),raster.max()))
		raster = 0
	return True

###################################################
def evaluateCloudCover(activity,config):
	print('evaluateCloudCover',activity)
	activity['cloudcover'] = 100
	finalcloudedareaand = None
	nodata = None
	dataset = None
# Get files in an ordered list (red,green,blue)
	for gband in config['cloudcover'][activity['inst']]:
		file = activity['files'][gband]
		logging.warning('evaluateCloudCover - {} reading {}'.format(activity['sceneid'],os.path.basename(file)))
		try:
			dataset = gdal.Open(file,GA_ReadOnly)
			raster = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.warning('evaluateCloudCover - {}'.format(gdal.GetLastErrorMsg()))
			raster = None
			return False
# Some images may have no valid pixels and will cause error on numpy.percentile
		if raster.min() == raster.max():
			logging.warning('evaluateCloudCover - No valid pixels in {}'.format(file))
			activity['message'] = 'No valid pixels in {}'.format(file)
			raster = None
			return False

# Start with a zeroed image imagearea
		if finalcloudedareaand is None:
			finalcloudedareaand = numpy.ones(raster.shape, dtype=numpy.bool_)
			finalcloudedareaor = numpy.zeros(raster.shape, dtype=numpy.bool_)
			logging.warning('evaluateCloudCover - {} band {} cloudedarea and {} or {}'.format(activity['sceneid'],gband,numpy.count_nonzero(finalcloudedareaand),numpy.count_nonzero(finalcloudedareaor)))
			template = config['publish'][activity['rp']][activity['inst']][gband]
			masked = activity['files'][gband].replace(template,'AMASK.tif')

# Evaluate nodata mask
			nodata = raster <= 0
# Create a cloudedarea mask with True where pixel value is above limit
		cloudedarea = raster > config['cloudcover'][activity['inst']][gband]
		logging.warning('evaluateCloudCover - {} band {} cloudedarea {} config {}'.format(activity['sceneid'],gband,numpy.count_nonzero(cloudedarea),config['cloudcover'][activity['inst']][gband]))
# Do an AND operation with the inverted accumulated cloud mask
		finalcloudedareaand = numpy.logical_and(cloudedarea,finalcloudedareaand)
# Do an OR operation with the accumulated cloud mask
		finalcloudedareaor = numpy.logical_or(cloudedarea, finalcloudedareaor)
		logging.warning('evaluateCloudCover - {} band {} cloudedarea {} and {} or {}'.format(activity['sceneid'],gband,numpy.count_nonzero(cloudedarea),numpy.count_nonzero(finalcloudedareaand),numpy.count_nonzero(finalcloudedareaor)))
		raster = None

# Evaluate cloud cover
	denominator = finalcloudedareaand.size-numpy.count_nonzero(nodata)
	numerator = numpy.count_nonzero(finalcloudedareaand)
	if denominator > 0:
		cloudcover = round(100. * numerator / denominator,1)
		activity['cloudcover'] = cloudcover
		logging.warning('evaluateCloudCover - {} finalcloudedarea {} - {}/{}/{}'.format(activity['sceneid'],cloudcover,numerator,denominator,finalcloudedareaor.size))

	try:
		driver = gdal.GetDriverByName('GTiff')
		cmdataset = driver.Create( masked, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Byte,  options = [ 'COMPRESS=LZW' ] )
		cmdataset.SetGeoTransform(dataset.GetGeoTransform())
		cmdataset.SetProjection( dataset.GetProjection() )
		cmdataset.GetRasterBand(1).SetNoDataValue(0)
		cmdataset.GetRasterBand(1).WriteArray( 255*finalcloudedareaand.astype(numpy.uint8) )
		cmdataset = None
	except Exception as e:
		activity['message'] = gdal.GetLastErrorMsg()
		logging.warning('evaluateCloudCover - {}'.format(gdal.GetLastErrorMsg()))

	return True

###################################################
@celery_app.task(queue='processing')
def generateTiles(activity):
	activity['task'] = 'generateTiles'
	if activity['status'] == 'ERROR':
		activity['message'] = 'Previous error'
		db_error(activity)
		return 	activity
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	config = loadConfig(activity['sat'])

# Evaluate limits for quicklook contrast stretch
	evaluateLimits(activity,config)
	driver = gdal.GetDriverByName('GTiff')
	mdriver = gdal.GetDriverByName('MEM')
	print('generateTiles',activity)

# Check if all tiles already exist
	alltilesexist = True
	newactivities = {}
	for v in range(2):
		newactivities[v] = {}
		for h in range(2):
			newactivities[v][h] = copy.deepcopy(activity)
			newtiffdir = os.path.join(activity['tiffdir'],'tiles/h{}v{}'.format(h,v))
			newactivities[v][h]['sceneid'] = '{}h{}v{}'.format(activity['sceneid'],h,v)
			newtiffdir = os.path.join(activity['tiffdir'],'tiles/h{}v{}'.format(h,v))
			newactivities[v][h]['tiffdir'] = newtiffdir
			if not os.path.exists(newtiffdir):
				os.makedirs(newtiffdir)
			for band in list(activity['files']):
				template = config['publish'][activity['rp']][activity['inst']][band]
				filename = os.path.basename(activity['files'][band]).replace(template,'h{}v{}_{}'.format(h,v,template))
				filename = os.path.join(newtiffdir,filename)
				srcmetadata = activity['metadata'][band]
				dstmetadata = filename.replace('tif','xml')
				if not os.path.exists(dstmetadata): 
					dest = shutil.copyfile(srcmetadata, dstmetadata) 
				newactivities[v][h]['files'][band] = filename
				newactivities[v][h]['metadata'][band] = dstmetadata
				if not os.path.exists(filename): 
					alltilesexist = False
				logging.warning('generateTiles - alltilesexist {} {}'.format(alltilesexist,filename))

	if not alltilesexist:
# For each one of the bands
		for band in list(activity['files']):
			try:
				dataset = gdal.Open(activity['files'][band],GA_ReadOnly)
				raster = dataset.GetRasterBand(1).ReadAsArray()
			except Exception as e:
				activity['message'] = 'Load {} - {}'.format(activity['files'][band],gdal.GetLastErrorMsg())
				logging.error('generateTiles - Load {} - {}'.format(activity['files'][band],gdal.GetLastErrorMsg()))
				dataset = None
				raster = None
				db_error(activity)
				return activity
# For each one of the tiles
			for v in range(2):
				for h in range(2):
					filename = newactivities[v][h]['files'][band]
					if os.path.exists(filename): 
						logging.warning('generateTiles - tile exists {}'.format(filename))
					else:
						logging.warning('generateTiles - tile creating {}'.format(filename))
						btype = dataset.GetRasterBand(1).DataType
						RasterYSize = int(dataset.RasterYSize/2)
						RasterXSize = int(dataset.RasterXSize/2)
						col1 = h * RasterXSize
						row1 = v * RasterYSize
						geotransform = dataset.GetGeoTransform()
						resolutionx = geotransform[1]
						resolutiony = geotransform[5]
						fulx = geotransform[0] + h * resolutionx * RasterXSize
						fuly = geotransform[3] + v * resolutiony * RasterYSize
						try:
							cmdataset = mdriver.Create('', RasterXSize, RasterYSize, 1, btype)
							cmdataset.SetGeoTransform([fulx, geotransform[1], 0, fuly, 0, geotransform[5]])
							cmdataset.SetProjection( dataset.GetProjection() )
							cmdataset.GetRasterBand(1).WriteArray( raster[row1:row1+RasterYSize,col1:col1+RasterXSize] )
							cmdataset.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
							dst_ds = driver.CreateCopy(filename, cmdataset, options=["COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=LZW"])
							dst_ds = None				
						except Exception as e:
							activity['message'] = 'Save {} - {}'.format(filename,gdal.GetLastErrorMsg())
							logging.error('generateTiles - Save {} - {}'.format(filename,gdal.GetLastErrorMsg()))
							dataset = None
							raster = None
							db_error(activity)
							return activity
			dataset = None
			raster = None
	for v in range(2):
		for h in range(2):
			newactivities[v][h]['task'] = 'generateQL'
			ret = db_launch(newactivities[v][h])
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
				return activity
			newactivities[v][h]['task'] = 'publishOneMS3'
			ret = db_launch(newactivities[v][h])
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
				return activity
			taskschain = chain([generateQL.s(newactivities[v][h]),publishOneMS3.s()])
			taskschain.apply_async()

# Publish the full scene
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	activity['task'] = 'generateQL'
	ret = db_launch(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	activity['task'] = 'publishOneMS3'
	ret = db_launch(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	taskschain = chain([generateQL.s(activity),publishOneMS3.s()])
	taskschain.apply_async()
	activity['task'] = 'generateTiles'
	return activity

###################################################
@celery_app.task(queue='processing')
def generateVI(activity):
	activity['task'] = 'generateVI'
	if activity['status'] == 'ERROR':
		activity['message'] = 'Previous error'
		db_error(activity)
		return 	activity
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	config = loadConfig(activity['sat'])

	print('generateVI',activity)
	template = '_'+config['publish'][activity['rp']][activity['inst']]['red']
	basename = activity['files']['red'].replace(template,'')
	ndviname = basename+"_NDVI.tif"
	eviname = basename+"_EVI.tif"
	if os.path.exists(ndviname) and os.path.exists(eviname):
		activity['message'] = 'Already done'
		db_end(activity)
		return activity
	activity['files']['ndvi'] = ndviname
	activity['files']['evi'] = eviname
	red = nir = blue = None
	try:
		dataset = gdal.Open(activity['files']['red'],GA_ReadOnly)
		RasterXSize = dataset.RasterXSize
		RasterYSize = dataset.RasterYSize
		red = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		dataset = gdal.Open(activity['files']['nir'],GA_ReadOnly)
		nir = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
		dataset = gdal.Open(activity['files']['blue'],GA_ReadOnly)
		blue = dataset.GetRasterBand(1).ReadAsArray(0, 0, dataset.RasterXSize, dataset.RasterYSize)
	except Exception as e:
		activity['message'] = gdal.GetLastErrorMsg()
		logging.error('generateVI - {}'.format(gdal.GetLastErrorMsg()))
		db_error(activity)
		return activity
# Create the ndvi image dataset if it not exists
	nodata = -9999
	nodatamask = red==nodata
	nir  = nir.astype(numpy.float32)/10000.
	red  = red.astype(numpy.float32)/10000.
	blue  = blue.astype(numpy.float32)/10000.
	fdriver = gdal.GetDriverByName('GTiff')
	mdriver = gdal.GetDriverByName('MEM')
	src_band = dataset.GetRasterBand(1)
	if not os.path.exists(ndviname):
		rasterndvi = (10000 * (nir - red) / (nir + red + 0.0001)).astype(numpy.int16)
		rasterndvi[nodatamask] = nodata
		try:
			vidataset = mdriver.Create('', dataset.RasterXSize, dataset.RasterYSize, 1, src_band.DataType)
			vidataset.SetGeoTransform( dataset.GetGeoTransform() )
			vidataset.SetProjection( dataset.GetProjection() )
			vidataset.GetRasterBand(1).WriteArray( rasterndvi )
			vidataset.GetRasterBand(1).SetNoDataValue(nodata)
			vidataset.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
			dst_ds = fdriver.CreateCopy(ndviname, vidataset, options=["COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=LZW"])
			dst_ds = None
			rasterndvi = None
			vidataset = None
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.warning('generateVI - {}'.format(gdal.GetLastErrorMsg()))
			db_error(activity)
			return activity

# Create the evi image dataset if it not exists
	if not os.path.exists(eviname):
		rasterevi = (10000 * 2.5 * (nir - red)/(nir + 6. * red - 7.5 * blue + 1)).astype(numpy.int16)
		rasterevi[nodatamask] = nodata
		try:
			vidataset = mdriver.Create('', dataset.RasterXSize, dataset.RasterYSize, 1, src_band.DataType)
			vidataset.SetGeoTransform( dataset.GetGeoTransform() )
			vidataset.SetProjection( dataset.GetProjection() )
			vidataset.GetRasterBand(1).WriteArray( rasterevi )
			vidataset.GetRasterBand(1).SetNoDataValue(nodata)
			vidataset.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
			dst_ds = fdriver.CreateCopy(eviname, vidataset, options=["COPY_SRC_OVERVIEWS=YES", "TILED=YES", "COMPRESS=LZW"])
			dst_ds = None
			rasterevi = None
			vidataset = None
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.warning('generateVI - {}'.format(gdal.GetLastErrorMsg()))
			db_error(activity)
			return activity
	
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	return activity

###################################################
@celery_app.task(queue='publish')
def publishOneMS3(activity):
	activity['task'] = 'publishOneMS3'
	if activity['status'] == 'ERROR':
		activity['message'] = 'Previous error'
		db_error(activity)
		return 	activity
	activity['message'] = ''
	ret = db_start(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
		return activity
	config = loadConfig(activity['sat'])

	logging.warning('publishOneMS3  sceneid {} {}'.format(activity['sceneid'],activity['dataset']))

# Check if dataset exists. If not create it.
	sql = "SELECT * FROM Dataset WHERE Name = '{}'".format(activity['dataset'])
	result = db_fetchone(sql)
	if result is None:
		description = '{} {} Level{} {} dataset'.format(activity['sat'],activity['inst'],activity['level'],activity['rp'])
		sql = "INSERT INTO Dataset (Name,Description) VALUES('%s', '%s')" % (activity['dataset'],description)
		db_execute(sql)

# Check if scene exists. If not, insert it. If yes, update it. Check again later because other rp may have been registered in the meantime
	sql = "SELECT * FROM Scene WHERE SceneId = '{0}'".format(activity['sceneid'])
	sresult = db_fetchone(sql)
	logging.warning('publishOneMS3 {} fetchone {}'.format(activity['sceneid'],sresult))

# Check if all products have already been registered. If yes, no need to register
	sql = "SELECT * FROM Product WHERE SceneId = '{0}' AND Dataset = '{1}'".format(activity['sceneid'],activity['dataset'])
	#sql = "SELECT * FROM Scene s, Product p WHERE s.SceneId = '{0}' AND p.SceneId = '{0}' AND p.Dataset = '{1}'".format(activity['sceneid'],activity['dataset'])
	presult = db_fetchall(sql)
	logging.warning('publishOneMS3 {} Products {}/{}'.format(activity['sceneid'],len(presult),len(activity['files'])))
	if len(presult) == len(activity['files']) and sresult is not None :
		activity['message'] = 'Already done'
		ret = db_end(activity)
		if ret is None: 
			activity['status'] = 'ERROR'
			activity['message'] = 'Connection error'
		return activity
# Get which bands have already been registered
	bandsdone = []
	for res in presult:
		bandsdone.append(res['Band'])

# Start filling metadata structure for tables Dataset, Scene and Product in Catalogo database
	result = {'Scene':{},'Product':{}}
	result['Product']['Dataset'] = activity['dataset']
	result['Scene']['SceneId'] = activity['sceneid']
	result['Scene']['Satellite'] = activity['sat']
	result['Scene']['Sensor'] = activity['inst']
	result['Scene']['Date'] = activity['date']
	result['Scene']['Path'] = activity['path']					
	result['Scene']['Row'] = activity['row']		
	result['Product']['SceneId'] = activity['sceneid']
	result['Product']['Resolution'] = config['resolution'][activity['inst']]

# Lets compute cloud cover. The best value comes from SR CMASK file (quality band)
# When SR has been calculated
	gdaldataset = None
	file = '' 
	if activity['rp'] == 'SR':
		if 'quality' not in activity['files']:
			activity['status'] = 'ERROR'
			activity['message'] = 'Quality file missing for scene in {}'.format(activity['tiffdir'])
			logging.error('publishOneMS3 - Quality file missing for scene in {}'.format(activity['tiffdir']))
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
		file = activity['files']['quality']
		try:
			gdaldataset = gdal.Open(file,GA_ReadOnly)
			raster = gdaldataset.GetRasterBand(1).ReadAsArray(0, 0, gdaldataset.RasterXSize, gdaldataset.RasterYSize)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.error('publishOneMS3 - {}'.format(gdal.GetLastErrorMsg()))
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
# Compute cloud cover
		unique, counts = numpy.unique(raster, return_counts=True)
		clear = 0.
		cloud = 0.
		for i in range(0,unique.shape[0]):
			if unique[i] == 127:
				clear = float(counts[i])
			if unique[i] == 255:
				cloud = float(counts[i])
		totalpixels = clear+cloud
		cloudcover = 100
		if totalpixels > 0:
			cloudcover = int(round(100.*cloud/(totalpixels),0))

		result['Scene']['CloudCoverMethod'] = 'ASR'
		result['Scene']['CloudCover'] = cloudcover
# Evaluate from DN products if scene has not yet been registered
	if activity['rp'] == 'DN' and sresult is None:
		file = list(activity['files'].values())[0]
		try:
			gdaldataset = gdal.Open(file,GA_ReadOnly)
		except Exception as e:
			activity['message'] = gdal.GetLastErrorMsg()
			logging.error('publishOneMS3 - {}'.format(gdal.GetLastErrorMsg()))
			ret = db_error(activity)
			if ret is None: 
				activity['status'] = 'ERROR'
				activity['message'] = 'Connection error'
			return activity
		activity['cloudcover'] = 0
		#evaluateCloudCover(activity,config)
		result['Scene']['CloudCoverMethod'] = 'ADN'
		result['Scene']['CloudCover'] = activity['cloudcover']

# Check if scene exists in SceneDataset. If not create it.
	sql = "SELECT * FROM SceneDataset WHERE SceneId = '{0}' AND Dataset = '{1}'".format(activity['sceneid'],result['Product']['Dataset'])
	sdresult = db_fetchone(sql)

# If scene has not been already registered
	if sresult is None or sdresult is None:
# Extract bounding box and resolution
		if gdaldataset is None: 
			file = list(activity['files'].values())[0]
			try:
				gdaldataset = gdal.Open(file,GA_ReadOnly)
			except Exception as e:
				activity['message'] = gdal.GetLastErrorMsg()
				logging.error('publishOneMS3 - {}'.format(gdal.GetLastErrorMsg()))
				ret = db_error(activity)
				if ret is None: 
					activity['status'] = 'ERROR'
					activity['message'] = 'Connection error'
				return activity
		geotransform = gdaldataset.GetGeoTransform()
		projection = gdaldataset.GetProjection()
		datasetsrs = osr.SpatialReference()
		datasetsrs.ImportFromWkt(projection)
		RasterXSize = gdaldataset.RasterXSize
		RasterYSize = gdaldataset.RasterYSize

		resolutionx = geotransform[1]
		resolutiony = geotransform[5]
		fllx = fulx = geotransform[0]
		fury = fuly = geotransform[3]
		furx = flrx = fulx + resolutionx * RasterXSize
		flly = flry = fuly + resolutiony * RasterYSize
# Create transformation from scene to ll coordinate

		llsrs = osr.SpatialReference()
		llsrs.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
		s2ll = osr.CoordinateTransformation ( datasetsrs, llsrs )

# Evaluate corners coordinates in ll
#	Upper left corner
		(ullon, ullat, nkulz ) = s2ll.TransformPoint( fulx, fuly)
#	Upper right corner
		(urlon, urlat, nkurz ) = s2ll.TransformPoint( furx, fury)
#	Lower left corner
		(lllon, lllat, nkllz ) = s2ll.TransformPoint( fllx, flly)
#	Lower right corner
		(lrlon, lrlat, nklrz ) = s2ll.TransformPoint( flrx, flry)

		result['Scene']['CenterLatitude'] = (ullat+lrlat+urlat+lllat)/4.
		result['Scene']['CenterLongitude'] = (ullon+lrlon+urlon+lllon)/4.

		result['Scene']['TL_LONGITUDE'] = ullon
		result['Scene']['TL_LATITUDE'] = ullat

		result['Scene']['BR_LONGITUDE'] = lrlon
		result['Scene']['BR_LATITUDE'] = lrlat

		result['Scene']['TR_LONGITUDE'] = urlon
		result['Scene']['TR_LATITUDE'] = urlat

		result['Scene']['BL_LONGITUDE'] = lllon
		result['Scene']['BL_LATITUDE'] = lllat
	
		logging.warning( 'publishOneMS3 bbox - {} ullon {} ullat {} fulx {} fuly {}'.format(activity['sceneid'],ullon, ullat,fulx, fuly))

# Get some metadata from xml file

	logging.warning('publishOneMS3 - metadata {}'.format(activity['metadata']))

	if len(activity['metadata']) > 0:
		xmlfile = list(activity['metadata'].values())[0]
		if os.path.exists(xmlfile):
			with open(xmlfile) as fp:
				f = fp.readlines()
				fstring = ' '.join(f)
				xml = xmltodict.parse(fstring)
				if 'prdf' in xml:
					if 'image' in xml['prdf']:
						if 'sunPosition' in xml['prdf']['image']:
							elevation =  xml['prdf']['image']['sunPosition']['elevation']
							sunAzimuth =  xml['prdf']['image']['sunPosition']['sunAzimuth']
						if 'syncLoss' in xml['prdf']['image']:
							syncLosses = []
							for sband in xml['prdf']['image']['syncLoss']['band']:
								syncLoss = float(sband['#text'])
								syncLosses.append(syncLoss)
							syncLoss = max(syncLosses)
							result['Scene']['SyncLoss'] = syncLoss
					if 'viewing' in xml['prdf']:
						StartTime = xml['prdf']['viewing']['begin'].replace('T',' ')[:19]
						CenterTime = xml['prdf']['viewing']['center'].replace('T',' ')[:19]
						StopTime = xml['prdf']['viewing']['end'].replace('T',' ')[:19]
						result['Scene']['Date'] = CenterTime[:10]
						result['Scene']['CenterTime'] = CenterTime
						result['Scene']['StartTime'] = StartTime
						result['Scene']['StopTime'] = StopTime
					if 'revolutionNumber' in xml['prdf']:
						revolutionNumber = xml['prdf']['revolutionNumber']
						#result['Scene']['Orbit'] = int(revolutionNumber)
				if 'rpdf' in xml:
					if 'sceneInfo' in xml['rpdf']:
						CenterTime = xml['rpdf']['sceneInfo']['centerTime'].replace('T',' ')[:19]
						result['Scene']['Date'] = CenterTime[:10]
						result['Scene']['CenterTime'] = CenterTime

# Inserting data into SceneDataset table
	if sdresult is None:
		keyval = {}
		keyval['SceneId'] = activity['sceneid']
		keyval['Dataset'] = activity['dataset']
		keyval['Date'] = result['Scene']['Date'] 
		keyval['C_Longitude'] = round(result['Scene']['CenterLongitude']*2,0)/2.
		keyval['C_Latitude'] = round(result['Scene']['CenterLatitude']*2,0)/2.
		params = ''
		values = ''
		for key,val in keyval.items():
			params += key+','
			if type(val) is str:
					values += "'{0}',".format(val)
			else:
					values += "{0},".format(val)
			
		sqlScene = "INSERT INTO SceneDataset ({0}) VALUES({1})".format(params[:-1],values[:-1])
		db_execute(sqlScene)

	result['Scene']['IngestDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")	
	result['Scene']['Deleted'] = 0
	result['Scene']['thumbnail'] = activity['pngname']

# Inserting data into Scene table
	sql = "SELECT * FROM Scene WHERE SceneId = '{0}'".format(activity['sceneid'])
	sresult = db_fetchone(sql)
	logging.warning('publishOneMS3 {} fetchone {}'.format(activity['sceneid'],sresult))
	if sresult is not None:
		params = ''
		for key,val in result['Scene'].items():
			if key == 'SceneId': continue
			if type(val) is str:
				params += "{} = '{}',".format(key,val)
			else:
				params += "{} = {},".format(key,val)
		sqlScene = "UPDATE Scene SET {} WHERE SceneId = '{}'".format(params[:-1],activity['sceneid'])
	else:
		params = ''
		values = ''
		for key,val in result['Scene'].items():
			params += key+','
			if type(val) is str:
					values += "'{0}',".format(val)
			else:
					values += "{0},".format(val)
			
		sqlScene = "INSERT INTO Scene ({0}) VALUES({1})".format(params[:-1],values[:-1])
	logging.warning( 'sqlScene - '+sqlScene)
	db_execute(sqlScene)

# Inserting data into Product table
	for band in config['publish'][activity['rp']][activity['inst']]:
		if band in bandsdone: continue
		file = activity['files'][band]
		ProcessingDate = datetime.datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
		result['Product']['ProcessingDate'] = ProcessingDate
		result['Product']['Band'] = band
		result['Product']['Filename'] = file
		params = ''
		values = ''
		for key,val in result['Product'].items():
				params += key+','
				if type(val) is str:
						values += "'{0}',".format(val)
				else:
						values += "{0},".format(val)

		sqlProduct = "INSERT INTO Product ({0}) VALUES({1})".format(params[:-1],values[:-1])
		#logging.warning( 'sqlProduct - '+sqlProduct)
		db_execute(sqlProduct)
	activity['status'] = ''
	ret = db_end(activity)
	if ret is None: 
		activity['status'] = 'ERROR'
		activity['message'] = 'Connection error'
	return activity

# ndvi.py
#
# Copyright(c) Exequiel Ceasar Navarrete <esnavarrete1@up.edu.ph>
# Licensed under MIT
# Version 0.0.0

from __future__ import unicode_literals

import os
import json
import glob
import random
import string
import shutil
import zipfile
import urllib2
import hashlib
import ee
from django.http import JsonResponse
from django.conf import settings
from django.views.decorators.cache import cache_page
from earth_engine import EE_CREDENTIALS, settings as ee_settings
from datetime import datetime, timedelta
from PIL import Image

#===================================================
# [Views] ::start
#===================================================

def index(request):
    return JsonResponse({
        'ndvi': 'I am NDVI api endpoint'
    })

# /ndvi/download-image-series/<start-date>/<end-date>?satellite=landsat-8&dimensions=256x256&province=Isabela
# satellites: landsat 8, sentinel 2, sentinel 1
@cache_page(60 * 60 * 24, cache="gee", key_prefix="ndvi_gee")
def download_image_series(request, startdate, enddate):
    province = request.GET.get('province', None)
    satellite = request.GET.get('satellite', 'landsat-8')
    dimensions = request.GET.get('dimensions', '256x256')

    # default values for satellites
    satellites = ['landsat-8', 'sentinel-1', 'sentinel-2']

    # default value for landsat 8 satellite interval
    satellite_interval = 16

    # return immediately if an invalid value for satellite is provided
    if not satellite in satellites:
        return JsonResponse({
            'success': False,
            'message': 'Invalid value for satellite. Allowed values are %s' % (', '.join(satellites))
        }, status=400)

    # assemble the string to be hashed for use in image and tmp folder name
    province_str = ''
    if province is not None:
        province_str = '-' + province

    secret_str = '%s-%s-%s-%s%s' % (startdate, enddate, satellite, dimensions, province_str)

    # concatenate the start date, end date and the random string and get the SHA 224 hash of it
    download_hash = hashlib.sha224(secret_str).hexdigest()

    # assemble the path where the processed images will be stored
    processed_image_folder = os.path.join(settings.STATIC_ROOT, 'earth-engine', download_hash)
    processed_images = []

    if os.path.exists(processed_image_folder):
        # fetch all images in the directory and append it to the processed images list
        for image in glob.glob(os.path.join(processed_image_folder, '*.jpg')):
            basename = os.path.splitext(image)[0]
            basename = os.path.basename(basename)

            processed_images.append({
                'date': basename,
                'url': request.META['HTTP_HOST'] + string.replace(image, os.getcwd(), '')
            })
    else:
        # modify the day intervals of the satellite
        if satellite == 'sentinel-1':
            satellite_interval = 12
        elif satellite == 'sentinel-2':
            satellite_interval = 10

        # get the list of possible date ranges starting from the start date to end date
        date_ranges = get_date_ranges_list(startdate, enddate, satellite_interval)

        # split the dimensions request parameter and cast to integer
        dimensions = map(int, dimensions.split('x'))

        # initialize connection to the earth engine
        ee.Initialize(EE_CREDENTIALS)

        download_settings = {
            'name': 'ndvi-' + satellite,
            'crs': 'EPSG:4326',
            'dimensions': dimensions,
            'region': get_par_geometry().getInfo()['coordinates']
        }

        # assemble the path where the downloaded files cound be saved
        tmp_path = os.path.join(os.getcwd(), 'data/tmp/ee-download', download_hash)

        if not os.path.exists(tmp_path):
            os.makedirs(tmp_path)

        # get the geometry of province and modify the region settings
        resolved_province = None
        if province is not None:
            resolved_province = get_province_geometry(province)

            download_settings['region'] = resolved_province.getInfo()['coordinates']

        images = []
        for date_range in date_ranges:
            # perform satellite image processing
            if satellite == 'landsat-8':
                image = process_landsat8_image_series(date_range['from'], date_range['to'], resolved_province)
            elif satellite == 'sentinel-2':
                image = process_sentinel2_image_series(date_range['from'], date_range['to'], resolved_province)
            else:
                pass

            # close the download settings and modify it
            dl_settings = download_settings.copy()
            dl_settings['name'] = 'ndvi-%s-%s-%s' % (satellite, date_range['from'], date_range['to'])

            download_url = image.getDownloadURL(dl_settings)

            downloaded_filename = os.path.join(tmp_path, dl_settings['name'] + '.zip')

            # download the files from the download url
            zip_file = urllib2.urlopen(download_url)
            with open(downloaded_filename, 'wb') as output:
                output.write(zip_file.read())

            images.append({
                'filename': downloaded_filename,
                'from': date_range['from'],
                'to': date_range['to']
            })

        # unzip all downloaded files
        for image in images:
            basename = os.path.splitext(image['filename'])[0]
            basename = os.path.basename(basename)

            extracted_folder_path = '%s/%s-extracted' % (tmp_path, basename)

            # extract the zip file in to the path specified
            with zipfile.ZipFile(image['filename'], 'r') as zip_ref:
                zip_ref.extractall(extracted_folder_path)

            # find all R, G and B images, combine them, and save to the static directory
            red = Image.open('%s/%s.vis-red.tif' % (extracted_folder_path, basename)).convert('L')
            blue = Image.open('%s/%s.vis-blue.tif' % (extracted_folder_path, basename)).convert('L')
            green = Image.open('%s/%s.vis-green.tif' % (extracted_folder_path, basename)).convert('L')

            processed_image_path = os.path.join(processed_image_folder, image['from'] + '.jpg')

            # create the folder inside the static folder
            if not os.path.exists(processed_image_folder):
                os.makedirs(processed_image_folder)

            # merge the separated channels to get the colored version
            out = Image.merge("RGB", (red, green, blue))
            out.save(processed_image_path)

            # asseble the the url pointing to the image
            processed_images.append({
                'date': image['from'],
                'url': request.META['HTTP_HOST'] + string.replace(processed_image_path, os.getcwd(), '')
            })

        # save some metadata for fetching the description later.
        # This can be further improved by saving this to a database.
        with open(os.path.join(processed_image_folder, 'metadata.json'), 'w') as metadata:
            json.dump({
                'satellite': satellite,
                'date_from': startdate,
                'date_to': enddate,
                'date_ranges': date_ranges
            }, metadata)

        # delete the tmp folder for the downloaded image
        shutil.rmtree(tmp_path)

    return JsonResponse({
        'success': True,
        'images': processed_images
    })

#===================================================
# [Views] ::end
#===================================================


#===================================================
# [View Helpers] ::start
#===================================================

def ndvi_mask(satellite):
    def mask(image):
        hansen_image = ee.Image('UMD/hansen/global_forest_change_2013')
        data = hansen_image.select('datamask')
        mask = data.eq(1)

        # generate different masks for different satellite data sources.
        if satellite == 'landsat-8':
            derived_mask = image.updateMask(mask)
        elif satellite == 'sentinel-2':
            derived_mask = image.select().addBands(image.normalizedDifference(['B8', 'B4'])).updateMask(mask)

        return derived_mask

    return mask

def get_province_geometry(province):
    ft = "ft:%s" % ee_settings.PROVINCES_FUSION_TABLES['LOCATION_METADATA_FUSION_TABLE']
    province_ft = ee.FeatureCollection(ft)

    found_province = ee.Filter.eq(ee_settings.PROVINCES_FUSION_TABLES['LOCATION_FUSION_TABLE_NAME_COLUMN'], province)

    return province_ft.filter(found_province).geometry()

def get_par_geometry():
    geometric_bounds = ee.List([
        [127.94248139921513, 5.33459854167601],
        [126.74931782819613, 11.825234466620996],
        [124.51107186428203, 17.961503806746318],
        [121.42999903167879, 19.993626604011016],
        [118.25656974884657, 18.2117821750514],
        [116.27168958893185, 6.817365082528201],
        [122.50121143769957, 3.79887124351577],
        [127.94248139921513, 5.33459854167601]
    ])

    return ee.Geometry.Polygon(geometric_bounds, 'EPSG:4326', True)

def get_date_ranges_list(start_date, end_date, interval):
    date_ranges = []

    startdate_py = datetime.strptime(start_date, '%Y-%m-%d')
    enddate_py = datetime.strptime(end_date, '%Y-%m-%d')

    # assemble the list of date ranges for getting data
    while True:
        if len(date_ranges) == 0:
            begin = startdate_py
        else:
            begin = datetime.strptime(date_ranges[-1]['to'], '%Y-%m-%d') + timedelta(days=1)

        newend = begin + timedelta(days=interval - 1)

        # append the last possible date range
        if newend > enddate_py:
            date_ranges.append({
                'from': begin.strftime('%Y-%m-%d'),
                'to': end_date,
            })

            break

        date_ranges.append({
            'from': begin.strftime('%Y-%m-%d'),
            'to': newend.strftime('%Y-%m-%d'),
        })

    return date_ranges

def process_landsat8_image_series(start_date, end_date, clipping_geometry=None):
    geometry = get_par_geometry()

    image_collection = ee.ImageCollection('LANDSAT/LC8_L1T_8DAY_NDVI')
    filtered = image_collection.filterDate(start_date, end_date).filterBounds(geometry).map(ndvi_mask('landsat-8'))

    # perform temporal reduction
    reduced = filtered.mean()

    # clip the image if clipping_geometry is provided
    if clipping_geometry is not None:
        reduced = reduced.clip(clipping_geometry)

    # return the visualized instance of the image
    return reduced.visualize(['NDVI'], None, None, 0, 1, None, None, [
        'FFFFFF', 'CE7E45', 'FCD163', '66A000', '207401', '056201', '004C00', '023B01', '012E01', '011301'
    ])

def process_sentinel2_image_series(start_date, end_date, clipping_geometry):
    geometry = get_par_geometry()

    image_collection = ee.ImageCollection('COPERNICUS/S2')
    filtered = image_collection.select(['B4', 'B8']).filterDate(start_date, end_date).filterBounds(geometry).map(ndvi_mask('sentinel-2'))

    # perform temporal reduction
    reduced = filtered.mean()

    # clip the image if clipping_geometry is provided
    if clipping_geometry is not None:
        reduced = reduced.clip(clipping_geometry)

    # return the visualized instance of the image
    return reduced.visualize(['nd'], None, None, 0, 1, None, None, [
        'FFFFFF', 'CE7E45', 'FCD163', '66A000', '207401', '056201', '004C00', '023B01', '012E01', '011301'
    ])

#===================================================
# [View Helpers] ::end
#===================================================



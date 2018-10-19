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
import urllib
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
# @cache_page(60 * 60 * 24, cache="gee", key_prefix="ndvi_gee")
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
            'truncated': False,
            'message': 'Invalid value for satellite. Allowed values are %s' % (', '.join(satellites))
        }, status=400)

    # assemble the string to be hashed for use in image and tmp folder name
    province_str = ''
    if province is not None:
        province_str = '-' + province

    secret_str = '%s-%s-%s-%s%s' % (startdate, enddate, satellite, dimensions, province_str)

    # concatenate the start date, end date and the random string and get the SHA 224 hash of it
    download_hash = hashlib.sha224(str(secret_str).encode('utf-8')).hexdigest()

    # assemble the path where the processed images will be stored
    processed_image_folder = os.path.join(settings.STATIC_ROOT, 'earth-engine', download_hash)
    processed_images = []

     # modify the day intervals of the satellite
    if satellite == 'sentinel-1':
        satellite_interval = 12
    elif satellite == 'sentinel-2':
        satellite_interval = 10

    # get the list of possible date ranges starting from the start date to end date
    date_ranges = get_date_ranges_list(startdate, enddate, satellite_interval)

    # flag to determine if date was truncated or not
    truncated = False

    # limit the date ranges to the value of the settings if it exceeds
    # and set the truncated flag to true
    max_num_image = ee_settings.NDVI['IMAGE_EXTRACTION']['MAX_IMAGES']
    if len(date_ranges) > max_num_image:
        date_ranges = date_ranges[0:max_num_image]

        truncated = True

    # check if folder is existing alreading for the requested images
    # if yes, skip processing and just return the images
    # if not, the do some processing
    if os.path.exists(processed_image_folder):
        images_glob = os.path.join(processed_image_folder, '*.' + ee_settings.NDVI['IMAGE_EXTRACTION']['IMAGE_FORMAT'])

        # fetch all images in the directory and append it to the processed images list
        for image in glob.glob(images_glob):
            basename = os.path.splitext(image)[0]
            basename = os.path.basename(basename)

            processed_images.append({
                'date': basename,
                'url': settings.STATIC_URL + os.path.relpath(image, settings.STATIC_ROOT + '/')
            })
    else:
        # split the dimensions request parameter and cast to integer
        dimensions = map(int, dimensions.split('x'))

        # initialize connection to the earth engine
        ee.Initialize(EE_CREDENTIALS)

        download_settings = {
            'name': 'ndvi-' + satellite,
            'crs': 'EPSG:4326',
            'dimensions': list(dimensions),
            'region': get_par_geometry().getInfo()['coordinates']
        }

        # assemble the path where the downloaded files cound be saved
        tmp_path = os.path.join(ee_settings.NDVI['IMAGE_EXTRACTION']['TMP_PATH'], download_hash)

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
                image = process_sentinel1_image_series(date_range['from'], date_range['to'], resolved_province)

            # close the download settings and modify it
            dl_settings = download_settings.copy()
            dl_settings['name'] = 'ndvi-%s-%s-%s' % (satellite, date_range['from'], date_range['to'])

            download_url = image.getDownloadURL(dl_settings)

            downloaded_filename = os.path.join(tmp_path, dl_settings['name'] + '.zip')

            # download the files from the download url
            zip_file = urllib.urlopen(download_url)
            with open(downloaded_filename, 'wb') as output:
                output.write(zip_file.read())

            images.append({
                'filename': downloaded_filename,
                'from': date_range['from'],
                'to': date_range['to']
            })

        # create the folder inside the static folder
        if not os.path.exists(processed_image_folder):
            os.makedirs(processed_image_folder)

        # unzip all downloaded files
        for image in images:
            basename = os.path.splitext(image['filename'])[0]
            basename = os.path.basename(basename)

            extracted_folder_path = '%s/%s-extracted' % (tmp_path, basename)

            # extract the zip file in to the path specified
            with zipfile.ZipFile(image['filename'], 'r') as zip_ref:
                zip_ref.extractall(extracted_folder_path)

            red_img_path = '%s/%s.vis-red.tif' % (extracted_folder_path, basename)
            blue_img_path = '%s/%s.vis-blue.tif' % (extracted_folder_path, basename)
            green_img_path = '%s/%s.vis-green.tif' % (extracted_folder_path, basename)
            gray_img_path = '%s/%s.vis-gray.tif' % (extracted_folder_path, basename)

            processed_image_path = os.path.join(processed_image_folder, image['from'] + '.' + ee_settings.NDVI['IMAGE_EXTRACTION']['IMAGE_FORMAT'])

            if os.path.exists(red_img_path) and os.path.exists(blue_img_path) and os.path.exists(green_img_path):
                # find all R, G and B images, combine them, and save to the static directory
                red = Image.open(red_img_path).convert('L')
                blue = Image.open(blue_img_path).convert('L')
                green = Image.open(green_img_path).convert('L')

                # merge the separated channels to get the colored version
                out = Image.merge("RGB", (red, green, blue))
            else:
                out = Image.open(gray_img_path).convert('L')

            out.save(processed_image_path)

            # asseble the the url pointing to the image
            processed_images.append({
                'date': image['from'],
                'url': settings.STATIC_URL + os.path.relpath(processed_image_path, settings.STATIC_ROOT + '/')
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
        'truncated': truncated,
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
        if satellite == 'landsat-8' or satellite == 'sentinel-1':
            derived_mask = image.updateMask(mask)
        elif satellite == 'sentinel-2':
            derived_mask = image.select().addBands(image.normalizedDifference(['B8', 'B4'])).updateMask(mask)

        return derived_mask

    return mask

def get_province_geometry(province):
    ft = "ft:%s" % ee_settings.PROVINCES_FUSION_TABLES['LOCATION_METADATA_FUSION_TABLE']
    province_ft = ee.FeatureCollection(ft)

    found_province = ee.Filter.eq(ee_settings.PROVINCES_FUSION_TABLES['LOCATION_FUSION_TABLE_NAME_COLUMN'], province)

    prov_geom = province_ft.filter(found_province).geometry()
    prov_info = prov_geom.getInfo()

    detected_max = prov_geom
    if prov_info['type'] == 'MultiPolygon':
        detected_max = None

        # get the largest polygon in the collection of MultiPolygon
        for coords in prov_info['coordinates']:
            polygon = ee.Geometry.Polygon(coords[0])

            if detected_max is None:
                detected_max = polygon
            elif detected_max.area() > polygon.area():
                detected_max = polygon

    return detected_max

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
            last_date = {
                'from': begin.strftime('%Y-%m-%d'),
                'to': end_date,
            }

            # make sure that the last date range from and to keys are not equal and of valid range!!!
            if last_date['from'] != end_date and begin < enddate_py:
                date_ranges.append(last_date)

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

def process_sentinel1_image_series(start_date, end_date, clipping_geometry):
    geometry = get_par_geometry()

    image_collection = ee.ImageCollection('COPERNICUS/S1_GRD')
    filtered = image_collection.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))

    reduced = filtered.select('VV').filterDate('2017-03-01', '2017-03-02').filterBounds(geometry).map(ndvi_mask('sentinel-1')).mosaic()

    # clip the image if clipping_geometry is provided
    if clipping_geometry is not None:
        reduced = reduced.clip(clipping_geometry)

    # return the visualized instance of the image
    return reduced.visualize(['VV'], None, None, -14, -9, None, None, [
        '011301', '012E01', '023B01', '004C00', '00ff00', '207401', '66A000', 'FCD163', 'CE7E45', 'FFFFFF'
    ])

#===================================================
# [View Helpers] ::end
#===================================================



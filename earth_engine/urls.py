# urls.py
#
# Copyright(c) Exequiel Ceasar Navarrete <esnavarrete1@up.edu.ph>
# Licensed under MIT
# Version 0.0.0

from django.conf.urls import url
from .views import ndvi

date_regex = '[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])'

urlpatterns = [
    url(r'ndvi$', ndvi.index, name='ee_ndvi_index'),
    url(r'ndvi/download-image-series/(?P<startdate>(' + date_regex + '))/(?P<enddate>(' + date_regex + '))$',
        ndvi.download_image_series,
        name='ee_ndvi_download_image_series')
]



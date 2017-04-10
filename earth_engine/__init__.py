# __init__.py
#
# Copyright(c) Exequiel Ceasar Navarrete <esnavarrete1@up.edu.ph>
# Licensed under MIT
# Version 0.0.0

import os
import sys
from oauth2client.service_account import ServiceAccountCredentials
from . import settings

#===================================================
# [Earth Engine Authorization] ::start
#===================================================

ee_api_config = settings.EARTH_ENGINE_API

if not os.path.exists(ee_api_config['PRIVATE_KEY']):
    print "Private key file not found on path: %s" % ee_api_config['PRIVATE_KEY']
    sys.exit(1)

EE_CREDENTIALS = ServiceAccountCredentials.from_p12_keyfile(ee_api_config['ACCOUNT'],
                                                            ee_api_config['PRIVATE_KEY'],
                                                            ee_api_config['KEY_SECRET'],
                                                            ee_api_config['SCOPES'])

#===================================================
# [Earth Engine Authorization] ::end
#===================================================



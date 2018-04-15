import datetime
import numpy as np
import pyart
import netCDF4
import sys
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['PATH'] = sys.argv[3] + sys.argv[4] + os.environ['PATH']

def read_mdv_radarCart(filename):
    filemetadata = pyart.config.FileMetadata('mdv', field_names=['DBZ_F'],
                                            additional_metadata=None,
                                            file_field_names=True, exclude_fields=None)
    mdv = pyart.io.mdv_common.MdvFile(pyart.io.common.prepare_for_read(filename))

    # time dictionaries
    units = pyart.io.common.make_time_unit_str(mdv.times['time_begin'])
    time = pyart.config.get_metadata('grid_time')
    time['data'] = np.array([netCDF4.date2num(mdv.times['time_centroid'], units)])
    time['units'] = units

    # origin dictionaries
    origin_altitude = pyart.config.get_metadata('origin_altitude')
    origin_altitude['data'] = np.array([mdv.master_header["sensor_alt"] * 1000.], dtype='float64')

    origin_latitude = pyart.config.get_metadata('origin_latitude')
    origin_latitude['data'] = np.array([mdv.master_header["sensor_lat"]], dtype='float64')

    origin_longitude = pyart.config.get_metadata('origin_longitude')
    origin_longitude['data'] = np.array([mdv.master_header["sensor_lon"]], dtype='float64')

    # grid coordinate dictionaries
    nz = mdv.master_header["max_nz"]
    ny = mdv.master_header["max_ny"]
    nx = mdv.master_header["max_nx"]
    z_line = mdv.vlevel_headers[4]["level"][0:nz]
    y_start = mdv.field_headers[4]["grid_miny"] * 1000.
    x_start = mdv.field_headers[4]["grid_minx"] * 1000.
    y_step = mdv.field_headers[4]["grid_dy"] * 1000.
    x_step = mdv.field_headers[4]["grid_dx"] * 1000.

    x = pyart.config.get_metadata('x')
    x['data'] = np.linspace(x_start, x_start + x_step * (nx - 1), nx)

    y = pyart.config.get_metadata('y')
    y['data'] = np.linspace(y_start, y_start + y_step * (ny - 1), ny)

    z = pyart.config.get_metadata('z')
    z['data'] = np.array(z_line, dtype='float64')

    # metadata
    metadata = filemetadata('metadata')
    for meta_key, mdv_key in pyart.io.mdv_common.MDV_METADATA_MAP.items():
        metadata[meta_key] = mdv.master_header[mdv_key]

    # fields
    fields = {}
    mdv_fields = ['DBZ_F']
    for mdv_field in set(mdv_fields):
        field_name = filemetadata.get_field_name(mdv_field)
        field_dic = filemetadata(field_name)
        field_dic['_FillValue'] = pyart.config.get_fillvalue()
        dataextractor = pyart.io.mdv_common._MdvVolumeDataExtractor(mdv, mdv.fields.index(mdv_field),
                                                        pyart.config.get_fillvalue(), two_dims=False)
        field_dic['data'] = dataextractor()
        fields[field_name] = field_dic

    return pyart.core.grid.Grid(time, fields, metadata, origin_latitude,
                                origin_longitude, origin_altitude, x, y, z)

mdvfile = sys.argv[1]
ncfile = sys.argv[2]
grid = read_mdv_radarCart(mdvfile)
pyart.io.write_grid(ncfile, grid)

delattr(sys.modules[__name__], 'grid')

# keck_tod.py
# ===========
# Read processed Keck Array time-ordered data.


import os.path
import numpy as np
import h5py as h5
import pandas as pd


class KeckTod:
    """
    Access one tag of Keck Array time-ordered data.

    A tag refers to a ~1 hour set of scans over fixed azimuth range and at
    constant elevation and deck (boresight) angle. The tag name is a 
    string like "20150614C06_dk023". This contains the date (2015-06-14), 
    the scanset identifier (phase C, scanset 6), and the telescope dk 
    angle (23 degrees).

    """
    
    
    def __init__(self, tag=None, prefix=None):
        """Constructor"""
        
        if tag is not None:
            self.read_tag(tag, prefix)

            
    def read_tag(self, tag, prefix):
        """Access data for specified tag."""
        
        self.tag = tag
        if prefix is None:
            self.tagdir = tag
        else:
            self.tagdir = prefix
        
        # Parse tag.
        self.year = int(tag[0:4])
        self.month = int(tag[4:6])
        self.day = int(tag[6:8])
        self.scanset = tag[8:11]
        self.dk = int(tag[14:17])
            
        # Open HDF5 file containing tod.
        todfile = os.path.join(self.tagdir, tag + '_tod.mat')
        self.tod = h5.File(todfile, 'r')
        
        # Read fp_data files.
        fp_master = pd.read_csv(os.path.join(self.tagdir, 'fp_data_master'),
                                comment='#', names=['fp_data', 'drum'])
        fp_data = []
        for (rx, fp_file, drum) in zip(range(len(fp_master)), fp_master.fp_data, fp_master.drum):
            fp_data.append(self.read_fp_data(fp_file, self.tagdir))
            fp_data[-1]['DRUM_ANGLE'] = np.float(drum)
            fp_data[-1]['THETA'] = fp_data[-1]['THETA'] - np.float(drum)
            fp_data[-1]['RX'] = rx
        self.fp_data = pd.concat(fp_data, ignore_index=True)

            
    def read_fp_data(self, datafile, filedir=''):
        """Read metadata describing instrument configuration."""

        # File headers are somewhat irregular and not all header
        # information is commented. Search through file to find where the
        # header actually starts.
        csvfile = open(os.path.join(filedir, datafile), 'r')
        header = 0
        for line in csvfile:
            if line.find('GCP') >= 0: break
            header = header + 1
        
        # Read fp_data csv files.
        fp_data = pd.read_csv(os.path.join(filedir, datafile),
                              header=header, skiprows=np.arange(2)+header+1)
        print('Read {} -- got {} lines'.format(datafile, len(fp_data)))
        # Strip whitespace out of column headings.
        fp_data = fp_data.rename(axis=1, mapper=lambda x: x.strip())

        # Several columns should be integer, but they contain nan to
        # indicate missing values. In python, nan is floating-point so we
        # replace these values with -1.
        # Pandas does include an integer nan, but it doesn't seem to be
        # very compatible.
        for column in ('TILE', 'DET_COL', 'DET_ROW'):
            fp_data[column] = pd.to_numeric(fp_data[column], errors='coerce')
            fp_data[column] = fp_data[column].fillna(value=-1)
            fp_data[column] = fp_data[column].astype(int)
        
        # String type columns need whitespace stripping
        for column in ('POL', 'TYPE', 'NIST_ROW', 'SMUX_SN', 'DET_ARR_SN'):
            fp_data[column] = fp_data[column].str.strip()

        # String type column that is mistakenly interpreted as float.
        for column in ('NYQ_SN', ):
            fp_data[column] = fp_data[column].astype(str)
            
        # Floating point columns with nan values.
        # Some of these columns seem to be all nan.
        for column in ('SQ1_ICMAX', 'DET_RES', 'DC_SHT_GND_BP1',
                       'DC_SHT_GND_BP2', 'ANT_SHT_GND', 'TES_INSP',
                       'ANT_INSP', 'PIX_PHYS_X', 'PIX_PHYS_Y',
                       'FWHM_MAJ', 'FWHM_MIN', 'R', 'THETA', 'ALPHA',
                       'CHI', 'EPSILON'):
            fp_data[column] = pd.to_numeric(fp_data[column], errors='coerce')

        # Done.
        return fp_data

    

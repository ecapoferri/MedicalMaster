import logging
import re
import traceback
from os import environ as os_environ
from pathlib import Path
from sys import stdout

import numpy as np
import numpy.typing as npt
from dotenv import load_dotenv
from os import chdir
from time import perf_counter
import configparser

PERF_START = perf_counter()

CALLING_DIR = Path.cwd()
# Must be set in env on host/container.
ROOT_PATH = Path(os_environ['APPS_ROOT'])
APP_PATH = ROOT_PATH / 'PM_LakeRepoRefresh'
chdir(APP_PATH)

load_dotenv(ROOT_PATH / '.env')
conf = configparser.ConfigParser()
conf.read(ROOT_PATH / 'app.cfg')
conf.read(ROOT_PATH / 'conn.cfg')
conf.read('.conf')

TOP_LOGGER_NAME = conf['DEFAULT']['LOGGER_NAME']

ANSI = '\x1b[{clr}m'
YLLW_ANSI = ANSI.format(clr='93')
RED_ANSI = ANSI.format(clr='1;91')
ANSI_RST = '\x1b[0m'
ANSI_LOG_PRFX = '\x1b[36m'

LOG_FMT_DATE_STRM = r'%y%m%d|%H%M'
LOG_FMT_DATE_FILE = r'%Y.%m.%d/%H.%M.%S'

# Regex pattern for cleaning up the loging output format string to be used as
#   headers for parsing content of the log file.
LOG_ATTR_PTN = r'[%\(\[\]]|\)[sd]'
# New line split character string is to help parse the log file. The log message
#   field will contain newline characters from some scripts. Should be something
#   that's unlikely to appear in a log message.
LOG_DELIM_FILE = '|'
LOG_NEWLINE_FILE = '==>>' 
LOG_FMT_FILE = (
    f"%(asctime)s"
    + f"{LOG_DELIM_FILE}%(name)s"
    + f"{LOG_DELIM_FILE}%(module)s"
    + f"{LOG_DELIM_FILE}%(process)d"
    + f"{LOG_DELIM_FILE}%(thread)d"
    + f"{LOG_DELIM_FILE}%(funcName)s"
    + f"{LOG_DELIM_FILE}[%(levelname)s]"
    + f"{LOG_DELIM_FILE}%(message)s"
    + f"{LOG_NEWLINE_FILE}"
)

LOG_FMT_STRM = (
    '{prf}%(asctime)s - %(name)s | '
    + '%(module)s:%(funcName)s{rst} {bld}>>{rst}\n'
    + '{lvl}{bld}[%(levelname)s] » {rst}{rst}%(message)s'
).format(prf=ANSI_LOG_PRFX, rst=ANSI_RST, lvl=YLLW_ANSI, bld='\x1b[1m')

HDLR_STRM = logging.StreamHandler(stdout)
HDLR_STRM.setFormatter(logging.Formatter(LOG_FMT_STRM, LOG_FMT_DATE_STRM))

LOG_FILE_NAME = f"{TOP_LOGGER_NAME}.log"
# File is overwritten when the module is run alone (__name__ = main).
HDLR_FILE = logging.FileHandler(
    filename=LOG_FILE_NAME, mode='a', encoding='utf-8')

HDLR_FILE.setFormatter(
    logging.Formatter(fmt=LOG_FMT_FILE, datefmt=LOG_FMT_DATE_FILE))

level_list = {
    logging.CRITICAL: 'logging.CRITICAL',
    logging.DEBUG: 'logging.DEBUG',
    logging.INFO: 'logging.INFO',
    logging.WARNING: 'logging.WARNING',
    logging.ERROR: 'logging.ERROR',
}

from typing import Any
def parse_logfile():
    # Split apart lines.
    lines = Path(LOG_FILE_NAME).read_text().split(f"{LOG_NEWLINE_FILE}\n")[:-1]
    # Split fields in lines.
    lines_split = [
        line.split(LOG_DELIM_FILE) for line in lines
    ]
    tbl_array = np.array(lines_split)
    # Make proper header strings by removing the formatting syntax from the
    #   first row, which is the logging output attributes.
    tbl_array[0] = [re.sub(LOG_ATTR_PTN, '', s) for s in tbl_array[0]]
    return tbl_array


def log_test(file_lvl=logging.DEBUG, stream_lvl=logging.WARNING):
    logger = logging.getLogger('log_test')
    try:

        HDLR_FILE.setLevel(file_lvl)
        HDLR_STRM.setLevel(stream_lvl)

        stream_level_str =\
            f"\nStream Handler Level: {level_list[HDLR_STRM.level]}"
        file_level_str = f"File Handler Level: {level_list[HDLR_FILE.level]}"

        for hdlr in (HDLR_FILE, HDLR_STRM):
            logger.addHandler(hdlr=hdlr)
        logger.setLevel(logging.DEBUG)

        print(stream_level_str, file_level_str, f"Stream Format:",
              LOG_FMT_STRM, '\n', sep='\n')
        logger.debug(f"Testing logging module...DEBUG")
        logger.info(f"Testing logging module...INFO")
        logger.warning(f"Testing logging module...WARNING")
        logger.error(f"Testing logging module...ERROR")
        logger.critical(f"Testing logging module...CRITICAL")
        print('')
        logger.warning(f"Log file, tabular:")
        for l in parse_logfile():
            print('\t', end='')
            print(*l, sep=' | ')
    except Exception:
        print(traceback.format_exc())
    finally:
        logger.debug(f"Execution time: {perf_counter() - PERF_START:.4f} s")
        for hdlr in (HDLR_FILE, HDLR_STRM): hdlr.close()
        return


if __name__ == "__main__":
    Path(LOG_FILE_NAME).write_text(f"{LOG_FMT_FILE}\n", encoding='utf-8')
    log_test()
chdir(CALLING_DIR)
import configparser
import logging
import re
import traceback
from os import chdir
from os import environ as os_environ
from pathlib import Path
from sys import stdout
from time import perf_counter
from typing import Any

import numpy as np
import numpy.typing as npt
from dotenv import load_dotenv

START = perf_counter()

load_dotenv('./.env')
load_dotenv('../.env')

CDW = Path().cwd()
chdir(os_environ['APP_PATH'])

config = configparser.ConfigParser()
config.read('.conf')


# TOP_LOGGER_NAME = os_environ['PRMDIA_LOGGER_NAME']
TOP_LOGGER_NAME = config['DEFAULT']['LOGGER_NAME']

ANSI = '\x1b[{clr}m'
YLLW_ANSI = ANSI.format(clr='93')
RED_ANSI = ANSI.format(clr='1;91')
ANSI_RST = '\x1b[0m'
ANSI_LOG_PRFX = ANSI.format(clr='96')

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

HDLR = logging.StreamHandler(stdout)
HDLR.setFormatter(logging.Formatter(LOG_FMT_STRM, LOG_FMT_DATE_STRM))

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


def parse_logfile() -> np.array:
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
    try:
        LOGGER = logging.getLogger('log_test')

        HDLR_FILE.setLevel(file_lvl)
        HDLR.setLevel(stream_lvl)

        stream_level_str =\
            f"\nStream Handler Level: {level_list[HDLR.level]}"
        file_level_str = f"File Handler Level: {level_list[HDLR_FILE.level]}"

        for hdlr in (HDLR_FILE, HDLR):
            LOGGER.addHandler(hdlr=hdlr)
        LOGGER.setLevel(logging.DEBUG)

        print(stream_level_str, file_level_str, f"Stream Format:",
              LOG_FMT_STRM, '\n', sep='\n')
        LOGGER.debug(f"Testing logging module...DEBUG")
        LOGGER.info(f"Testing logging module...INFO")
        LOGGER.warning(f"Testing logging module...WARNING")
        LOGGER.error(f"Testing logging module...ERROR")
        LOGGER.critical(f"Testing logging module...CRITICAL")
        print('')
        LOGGER.warning(f"Log file, tabular:")
        for l in parse_logfile():
            print('\t', end='')
            print(*l, sep=' | ')
    except Exception:
        LOGGER.debug(traceback.format_exc())
    finally:
        for hdlr in (HDLR_FILE, HDLR):
            hdlr.close()
        return


if __name__ == "__main__":
    try:
        Path(LOG_FILE_NAME).write_text(f"{LOG_FMT_FILE}\n", encoding='utf-8')
        log_test()
    finally:
        HDLR.close()

chdir(CDW)
#!/usr/bin/env python
# coding=utf-8

from ibats_utils.mess import try_n_times

from lib.utility.log import *
from lib.zone.area import *


# response = requests.get(page, timeout=10, headers=headers)
@try_n_times(times=10, sleep_time=0.025, logger=logger, exception=Exception, exception_sleep_time=6)
def invoke_web_page(page, headers):
    response = requests.get(page, timeout=10, headers=headers)

    return response

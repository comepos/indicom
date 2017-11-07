__author__ = 'stephane.ploix@g-scop.grenoble-inp.fr'

import datetime
import time

def epochtimems_to_stringdate(epochtimems: int):
    """
    transform an epoch time  into a string representation
    :param epochtimems: epoch time in milliseconds
    :return: string representation '%d/%m/%Y %H:%M:%S'
    """
    return time.strftime('%d/%m/%Y %H:%M:%S', time.localtime(epochtimems // 1000))


def epochtimems_to_datetime(epochtimems: int):
    """
    transform an epoch time into an internal datetime representation
    :param epochtimems: epoch time in milliseconds
    :return: internal datetime representation
    """
    return datetime.datetime.fromtimestamp(epochtimems // 1000)

def pdsTimestamp_to_epochtimems(stringdatetime: str):
    epochdatems = time.mktime(time.strptime(stringdatetime, '%Y-%m-%d %H:%M:%S')) * 1000
    return int(epochdatems)

def stringdate_to_epochtimems(stringdatetime: str):
    """
    transform a date string representation into an epoch time
    :param stringdatetime: date string representation '%d/%m/%Y %H:%M:%S'
    :return: epoch time in milliseconds
    """
    epochdatems = time.mktime(time.strptime(stringdatetime, '%d/%m/%Y %H:%M:%S')) * 1000
    return int(epochdatems)

def separatedStringDate_to_epochtimems(year: str, month:str, day:str, hour:str, minute:str, second:str):
    stringdatetime=day+'/'+month+'/'+year+' '+hour+':'+minute+':'+second
    epochdatems = time.mktime(time.strptime(stringdatetime, '%d/%m/%Y %H:%M:%S')) * 1000
    return int(epochdatems)

def stringdate_to_datetime(stringdatetime: str):
    """
    transform a date string representation into an internal datetime representation
    :param stringdatetime: date string representation '%d/%m/%Y %H:%M:%S'
    :return: internal datetime representation
    """
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(stringdatetime, '%d/%m/%Y %H:%M:%S')))


def epochtimems_to_timequantum(epochtimems: int, timequantum_duration_in_secondes: int):
    """
    transform an epoch time into a rounded discrete epoch time according to a given time quantum (sampling period)
    :param epochtimems: epoch time in milliseconds
    :param timequantum_duration_in_secondes: time quantum duration (sampling period) in seconds
    :return: rounded discrete epoch time in milliseconds
    """
    return (epochtimems // (timequantum_duration_in_secondes * 1000)) * timequantum_duration_in_secondes * 1000


def get_stringdate_with_day_delta(numberofdays: int=0):
    return (datetime.datetime.now() - datetime.timedelta(days=numberofdays)).strftime('%d/%m/%Y %H:%M:%S')
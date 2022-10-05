#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))


# /!\ All functions below must return an RDD object /!\

# T1: replace pass with your code
def extract_email_network(rdd):
    
    rdd_mail = rdd.map(Parser().parsestr) 
    
    
    def get_recipient_list(x):
    
        "Gets as recipients of an email into list format"
    
        To = x.get('To')
        Cc = x.get('Cc')
        Bcc = x.get('Bcc')
    
        if To != None:
            rec_list = To.split(', ')
        else:
            rec_list = []
    
        if Cc != None:
            rec_list.extend(Cc.split(', '))
    
        if Bcc != None:
            rec_list.extend(Bcc.split(', '))
    
        return rec_list
    
    rdd_full_email_tuples = rdd_mail.map(lambda x: (x.get('From'), get_recipient_list(x), date_to_dt(x.get('Date'))))
    
    rdd_email_triples = rdd_full_email_tuples.flatMap(lambda x: map(lambda iterator: (x[0], iterator, x[2]), x[1]))
    
    email_regex = "[A-Za-z0-9!#$%&*+-/=?^_`{|}~'.]+\@([A-Za-z0-9]+\.([A-Za-z0-9]+\.)*[a-zA-Z]{1}([a-zA-Z0-9])*|[a-zA-Z]{1}([a-zA-Z0-9])*)"
    valid_email = lambda s: re.compile(email_regex).fullmatch(s) != None

    ending_regex = "\S+enron.com"
    valid_ending = lambda s: re.compile(ending_regex).fullmatch(s) != None

    rdd_email_triples_enron = rdd_email_triples.filter(lambda x: valid_email(x[0]))\
                                               .filter(lambda x: valid_email(x[1]))\
                                               .filter(lambda x: valid_ending(x[0]))\
                                               .filter(lambda x: valid_ending(x[1]))
                
    distinct_triples = rdd_email_triples_enron.distinct()
    
    return distinct_triples 

# T2: replace pass with your code            
def get_monthly_contacts(rdd):
    
    def mmyyyy(dt_obj):
        """
        This function takes a datetime object and returns a string in the format MM/YYYY
        """
    
        mon = dt_obj.month
    
        yr = dt_obj.year
    
        return str(mon) + '/' + str(yr)
    
    rdd = rdd.map(lambda t: ((t[0], mmyyyy(t[2])), 1))\
             .reduceByKey(lambda a, b: a + b)\
             .map(lambda t: (t[0][0], t[0][1], t[1]))\
             .sortBy(lambda t: t[0], False)\
             .sortBy(lambda t: t[2], False)
            
    
    return rdd

# T3: replace pass with your code
def convert_to_weighted_network(rdd, drange = None):
    
    if drange != None:
        rdd = rdd.filter(lambda t: t[2] >= drange[0])\
                 .filter(lambda t: t[2] <= drange[1])
    
    rdd = rdd.map(lambda t: ((t[0], t[1]), 1))\
             .reduceByKey(lambda t1, t2: t1 + t2)\
             .map(lambda t: (t[0][0], t[0][1], t[1]))
    
    return rdd

# T4.1: replace pass with your code
def get_out_degrees(rdd):
    
    rdd_sent = rdd.map(lambda t: t[0])
    rdd_rec = rdd.map(lambda t: t[1])
    
    # get distnct emails in the form (email, 0)
    rdd_emails = rdd_sent.union(rdd_rec)\
                         .distinct()\
                         .map(lambda s: (s, 0))
            
    rdd = rdd.map(lambda t: (t[0], t[2]))\
             .union(rdd_emails)\
             .reduceByKey(lambda a,b: a + b)\
             .map(lambda t: (t[1], t[0]))\
             .sortBy(lambda t: t[1], False)\
             .sortBy(lambda t: t[0], False)
        
    return rdd

# T4.2: replace pass with your code         
def get_in_degrees(rdd):
    
    rdd_sent = rdd.map(lambda t: t[0])
    rdd_rec = rdd.map(lambda t: t[1])
    
    # get distnct emails in the form (email, 0)
    rdd_emails = rdd_sent.union(rdd_rec)\
                         .distinct()\
                         .map(lambda s: (s, 0))
            
    rdd = rdd.map(lambda t: (t[1], t[2]))\
             .union(rdd_emails)\
             .reduceByKey(lambda a,b: a + b)\
             .map(lambda t: (t[1], t[0]))\
             .sortBy(lambda t: t[1], False)\
             .sortBy(lambda t: t[0], False)
        
    return rdd


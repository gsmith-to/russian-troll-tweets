import codecs
import csv
import sqlite3
import os
import re
import time

#   Read IRAhandle_tweets_1.csv through IRAhandle_tweets_13.csv
#   and generate sqlite database IRAhandle_tweets.db
#
# fields are the same, noting
#   'content' is unicode;
#   'publish_date' and 'harvested_date' are reformatted to yyyy/mm/dd hh:mm
#    so that string compares will work.
#   'publish_time' is added; this is publish_date converted to seconds.
#     The times in the csv are all UTC (confirmed, issue #9)
#   'external_author_id' field: as of 27-Aug-2018 this is integer type,
#    up to 18 digits; have changed it to 'integer' (sqlite3 supports up to
#    64 bits for integer).
#   'tco{1,2,3}_step1' fields are combined to 'tco_step1' which is a blank-separated concat
#    of the non-empty urls.
#   'article_url' and the tco urls have commonly used prefixes abbreviated as described below.
#    (This can be disabled).
#
# Indices are added on: author, region, language, publish_time, account_type, alt_external_id, tweet_id.
# This typically takes a few minutes to run.
#
# NOTE: Resulting db file is about 1.6GB (it will be smaller if you remove indices).
# However, sqlitebrowser uses > 5GB of RAM just to open it. Apparently sqlitebrowser is
# not well designed to cope with large databases...
# 
# Optional URL prefix shortening applied to fields article_url, tco1_step1, tco2_step2, tco2_step3:
#   - prefix of https://twitter.com/        replaced with @T/
#   - prefix of http://twitter.com/         replaced with @t/
#   - else prefix of https://         replaced with @H/
#   - else prefix of http://          replaced with @h/
#
ShortenUrlPrefix = True

re_date = re.compile(r"\s*(\d+)/(\d+)/(\d+)\s+(\d+):(\d+)\s*")

base_time = time.mktime( (1970,1,2, 0,0,0, 0,0,0)) - 24*3600

# convert m/d/yyyy hh:mm to yyyy/mm/dd hh:mm and optionally to  time-since-1970-UTC
def convert_date( dstrng, tz= 0, only_reformat=False):
    m = re_date.match( dstrng )
    if m is None:
        raise ValueError("bad time string %s" %dstrng)
    mm,dd,yy,thh,tmm = [ int(x,10) for x in m.groups()]
    newstring = "%d/%02d/%02d %02d:%02d" %( yy,mm,dd,thh,tmm)
    if only_reformat:
        return newstring
		
    toff = time.mktime( (yy,mm,dd, thh, tmm, 0, 0, 0, 0) ) -base_time + 3600*tz
    return newstring,int(round(toff))
#
# read the CSV file, make sqlite database
#
def copy_to_db( infilenames, outfilename):
    try:
        os.unlink(outfilename)
    except OSError:
        pass

    conn = sqlite3.connect(outfilename)
    
    c = conn.cursor()
    c.execute( "pragma encoding='UTF-8';")
    # Create table
    c.execute('''CREATE TABLE tweets (
        external_author_id	integer, ''' # 'integer is up to 64 bits in sqlite3
  '''   author              text,
        content             text,
        region              text,
        language            text,
        publish_date        text,
        publish_time        integer,
        harvested_date      text,
        following           integer,
        followers           integer,
        updates             integer,
        post_type           text,
        account_type        text,
        retweet             integer,
        account_category    text,
        new_june_2018       integer,
        alt_external_id     integer,
        tweet_id            integer,
        article_url         text,
        tco_step1           text
    )''')
    c.execute("CREATE INDEX tweet_author ON tweets( author )")
    c.execute("CREATE INDEX tweet_region ON tweets( region )")
    c.execute("CREATE INDEX tweet_language ON tweets( language )")
    c.execute("CREATE INDEX tweet_time ON tweets( publish_time )")
    c.execute("CREATE INDEX tweet_account_type ON tweets( account_type )")
    c.execute("CREATE INDEX tweet_alt_external_id ON tweets( alt_external_id )")
    c.execute("CREATE INDEX tweet_tweed_id ON tweets( tweet_id )")


    fields = ("external_author_id author content region language publish_date publish_time "
        "harvested_date following followers updates post_type account_type retweet "
        "account_category new_june_2018 alt_external_id tweet_id article_url tco_step1").split()

    cmd = "INSERT INTO tweets VALUES (" + ("?,"*len(fields))[:-1] + ")"

    n = 0
    for fn in infilenames:
        print "file %s; %d so far" %( fn,n)
        with open(fn) as csvfile:
            reader = csv.DictReader( csvfile)
            for rec in reader:
                urls = convert_urls( [rec['article_url'],rec['tco1_step1'],rec['tco2_step1'],rec['tco3_step1']])
                rec['article_url'] = urls[0]
                rec['tco_step1'] = combine_tco(urls[1:4])
                rec['publish_date'],rec['publish_time'] = convert_date( rec['publish_date'])
                rec['harvested_date'] = convert_date( rec['harvested_date'],only_reformat=True)
                rec['content'] = codecs.decode( rec['content'],'utf_8')
                out_fields = [ rec[x] for x in fields]
                try:
                    c.execute( cmd, out_fields  )
                except Exception:
                    print out_fields
                    raise
                n += 1
    conn.commit()
    print "%d records copied to %s from %d source files" % (n, outfilename, len(infilenames) )

def convert_urls(urls):
    res = []
    for u in urls:
        if ShortenUrlPrefix:
           u = shorten_url_prefix(u)
        u = codecs.decode(u, 'utf_8')
        res.append(u)
    return res

def shorten_url_prefix(s):
    if s.startswith('https://'):
        if s[8:20] == 'twitter.com/':
            return '@T/' + s[20:]
        return '@H/'+s[8:]
    if s.startswith('http://'):
        if s[7:19] == 'twitter.com/':
            return '@t/' + s[19:]
        return '@h/'+s[7:]
    return s

def combine_tco( tlst ):
    return ' '.join( filter( None, tlst))


infnames = [ "IRAhandle_tweets_%d.csv" % k for k in range(1,13+1)]

copy_to_db(infnames,"IRAhandle_tweets.db")

 

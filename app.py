#!/usr/bin/python
import psycopg2
import psycopg2.extras
import requests
import sys
from time import sleep

def main():
    #Define our connection string
    conn_string = "host='localhost' port=5433 dbname='CLeagueHero' user='postgres'"

    # print the connection string we will use to connect
    print "Connecting to database\n ->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    conn2 = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor2 = conn2.cursor()
    print "Connected!\n"

    # execute our Query
    cursor.execute("SELECT * FROM arenas WHERE coords IS NULL")

    f = open('results.txt', 'w')
    for record in cursor:
        # print record[0]
        try:
            address = "%s %s" % (record["address"], record["address_2"])
            r = requests.get("http://maps.googleapis.com/maps/api/geocode/json?address=%s" % address)
            f.write("Got response %s\n" % r.status_code)
            f.write("Address %s\n" % address)
            j = r.json()
            # f.write(r.text + "\n")
            if(j["results"]):
                lat = j["results"][0]["geometry"]["location"]["lat"]
                lng = j["results"][0]["geometry"]["location"]["lng"]
                f.write("Lat Long %s %s\n" % (lat, lng))
                s = 'UPDATE arenas SET coords = ST_GeomFromText(\'POINT(%s %s)\', 4326) WHERE id = %s;' % (lng, lat, record["id"])
                f.write("%s\n" % s)
                f.write("\n")

                cursor2.execute(s)
                conn2.commit()
            sleep(.1)
        except:
            print "Unexpected error:", sys.exc_info()[0]

    conn2.close()


if __name__ == "__main__":
    main()

#!/usr/bin/python
import psycopg2
import psycopg2.extras
import requests
import sys
from time import sleep

def update_geo_location(cursor, id, lat=0, lng=0):
    s = 'UPDATE arenas SET coords = ST_GeomFromText(\'POINT(%s %s)\', 4326) WHERE id = %s;' % (lng, lat, str(id))
    cursor.execute(s)
    return s


def main():
    #Define our connection string
    conn_string = "host='localhost' port=32768 dbname='postgres' user='postgres' password='mysecretpassword'"

    # print the connection string we will use to connect
    print "Connecting to database\n ->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    with psycopg2.connect(conn_string) as conn:

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            print "Connected!\n"

            # execute our Query
            cursor.execute("SELECT * FROM arenas WHERE coords IS NULL")

            with open('results.txt', 'w') as f:
                for record in cursor:
                    # print record[0]
                    try:
                        address = "%s" % (record["address"])
                        r = requests.get("http://maps.googleapis.com/maps/api/geocode/json?address=%s" % address)
                        f.write("Got response %s\n" % r.status_code)
                        f.write("Address %s\n" % address)
                        j = r.json()
                        # f.write(r.text + "\n")
                        if(j["results"]):
                            lat = j["results"][0]["geometry"]["location"]["lat"]
                            lng = j["results"][0]["geometry"]["location"]["lng"]
                            f.write("Lat Long %s %s\n" % (lat, lng))

                            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as update:
                                s = update_geo_location(update, record['id'], lng, lat)
                                f.write("%s\n" % s)
                                f.write("\n")
                                conn.commit()
                        sleep(.1)
                    except:
                        print "Unexpected error:", sys.exc_info()[0]
                        raise

if __name__ == "__main__":
    main()

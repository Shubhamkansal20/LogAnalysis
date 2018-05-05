#!/usr/bin/env python2.7

import psycopg2


output = open('output.txt', 'w+')


def connect():
    return psycopg2.connect("dbname=news")


def stat1():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""select count(*) as cnt,articles.title
        from log,articles where
        log.path='/article/'||articles.slug
        group by articles.title
        order by cnt desc limit 3;""")
    count = cur.fetchall()
    output.write("What are the most popular three articles of all time?\n")
    for item in count:
        output.write('"'+str(item[1])+'"'+' - '+str(item[0])+" views\n")
    output.write("\n")
    d_b.close()


def stat2():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""select count(*) as cnt, authors.name
        from log,articles,authors
        where authors.id = articles.author and
        log.path = '/article/'||articles.slug
        group by authors.name
        order by cnt desc;""")
    count = cur.fetchall()
    output.write("Who are the most popular article authors of all time?\n")
    for item in count:
        output.write(str(item[1])+' - '+str(item[0])+" views\n")
    output.write("\n")
    d_b.close()


def stat3():
    d_b = connect()
    cur = d_b.cursor()
    cur.execute("""create view total as
        select count(*) as total,date(time)
        from log
        group by date(time)
        order by date(time);""")
    cur.execute("""create view errors as
        select count(*) as errors,date(time)
        from log
        where status like '4%'
        group by date(time)
        order by date(time);""")
    cur.execute("""create view percentage as
        select (cast(errors.errors as float)/cast(total.total as float))*100
        as percentage,total.date
        from errors natural join total;""")
    cur.execute("""select * from percentage
        where percentage>1;""")
    count = cur.fetchall()
    output.write("Which days did more than 1% of requests lead to errors?\n")
    for item in count:
        output.write(str(item[1])+' - '+"%.2f" % item[0]+"% errors\n")
    output.write("\n")
    d_b.close()

stat1()
stat2()
stat3()

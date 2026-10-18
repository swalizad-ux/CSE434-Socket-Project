import random, sys
random.seed(7)
hdr = ("BEGIN_YEARMONTH,BEGIN_DAY,BEGIN_TIME,END_YEARMONTH,END_DAY,END_TIME,EPISODE_ID,EVENT_ID,STATE,STATE_FIPS,"
       "YEAR,MONTH_NAME,EVENT_TYPE,CZ_TYPE,CZ_FIPS,CZ_NAME,WFO,BEGIN_DATE_TIME,CZ_TIMEZONE,END_DATE_TIME,"
       "INJURIES_DIRECT,INJURIES_INDIRECT,DEATHS_DIRECT,DEATHS_INDIRECT,DAMAGE_PROPERTY,DAMAGE_CROPS,TOR_F_SCALE")
must = [5536849, 2402920, 5539287]
ids = set(must)
while len(ids) < 4000:
    ids.add(random.randint(2000000, 5600000))
ids = list(ids); random.shuffle(ids)
states = ["GEORGIA","TEXAS","NEW YORK","OHIO"]
with open("details-1996.csv", "w") as f:
    f.write(hdr + "\n")
    for e in ids:
        st = random.choice(states)
        f.write(f'199601,{random.randint(1,28)},1200,199601,{random.randint(1,28)},1300,{e//10},{e},"{st}",{random.randint(1,50)},'
                f'1996,January,Tornado,C,{random.randint(1,200)},"{st} COUNTY, NORTH",FFC,01-JAN-96 12:00:00,EST,01-JAN-96 13:00:00,'
                f'0,0,0,0,10.00K,0.00K,EF0\n')
print("rows:", len(ids))

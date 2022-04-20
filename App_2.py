import pymongo
from datetime import datetime

def date_calc(some_date):
    Year = int(some_date.split('-')[0])
    month = int(some_date.split('-')[1])
    day = int(some_date.split('-')[2])
    hours = int(some_date.split('-')[3])
    minutes = int(some_date.split('-')[4])
    return Year,month,day,hours,minutes


def check_date(date_var):
    try:
        _date = input(f"Please give a {date_var} date in the following format YYYY-MM-DD-HH-MinMin \n The starting date and end date should be within the time interval 1970-01-01 and 2022-04-10 \n where YYYY = YEAR (4 Digits) \n MM = Month (2 digits) \n DD = Day (2 digits) \n HH = Hour (2 digits) \n MinMin = Minutes (2 digits) \n The numbers should be separated by dash \n Give Input here :") 
        _Y,_M,_D,_H,_min = date_calc(_date)
    except:
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        _Y,_M,_D,_H,_min = check_date(date_var)
    return _Y,_M,_D,_H,_min

def finacl_check(val):
    Y,M,D,H,minim = check_date(val)
    if (len(str(Y))!=4):
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        Y,M,D,H,minim = check_date(val)
    elif(len(str(M))>2):
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        Y,M,D,H,minim = check_date(val)
    elif(len(str(D))>2):
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        Y,M,D,H,minim = check_date(val)
    elif(len(str(H))>2):
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        Y,M,D,H,minim = check_date(val)
    elif(len(str(minim))>2):
        print(" ")
        print('AN error occured your input is not correct please follow the instructions')
        Y,M,D,H,minim = check_date(val)
    return datetime(Y,M,D,H,minim, 0, 0)

def compare_dates():
    start_max_date = datetime(1970,1,1,0,0)
    end_max_date = datetime(2022,4,11,0,0)
    start_date = finacl_check('STARTING')
    print(" ")
    end_date = finacl_check('END')
    if end_date<start_date:
        print(" ")
        print("End date connot be before start date")
        start_date,end_date= compare_dates()
    if start_date < start_max_date:
        print(" ")
        print("Start date cannot be earlier than 1970-01-01")
        start_date,end_date= compare_dates()
    elif end_date > end_max_date:
        print(" ")
        print("End date cannot be later than 2022-04-10")
        start_date,end_date= compare_dates()
    
    return start_date,end_date

client=pymongo.MongoClient('localhost',27017)
db = client.itc6107
coll = db.StockExachange

start,end = compare_dates()
 
#Questions 1 and 3 are answered on output_1

print("Average,Max,Min price and SPREAD per share ")
print(" ")
output_1=coll.aggregate(
    [
     {'$match':{'TS':{'$gt': start,'$lte':end}}},
     {'$group':{'_id': '$TICK',
      'Average_Price':{'$avg':'$PRICE'},
      'Max_Price':{'$max':'$PRICE'},
      'Min_Price':{'$min':'$PRICE'}
      }},
     {
      '$addFields':{'SPREAD':{'$divide':[{'$subtract':['$Max_Price','$Min_Price']},'$Average_Price']}}
      }
     ]
    )

for i in output_1:
    print(i)

print(" ")

print("Last update per share")
output_2=coll.aggregate(
    [
     {'$match':{'TS':{'$gt': start,'$lte':end}}},
     {'$group':{'_id': '$TICK',
      'Last Update':{'$max':'$TS'}
      }}
     ]
    )
for i in output_2:
    print(i)
    
print(" ")


print("Minimum Spread")
print(" ")
output_3=coll.aggregate(
    [
     {'$match':{'TS':{'$gt': start,'$lte':end}}},
     {'$group':{'_id': '$TICK',
      'Average_Price':{'$avg':'$PRICE'},
      'Max_Price':{'$max':'$PRICE'},
      'Min_Price':{'$min':'$PRICE'}
      }},
     {
      '$addFields':{'SPREAD':{'$divide':[{'$subtract':['$Max_Price','$Min_Price']},'$Average_Price']}}
      },
     {
      '$sort':{'SPREAD':1}
      },
     {
      '$limit':1
      }
     ]
    )

for i in output_3:
    print(i)

    
    
    



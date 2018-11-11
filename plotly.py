import csv
import psycopg2
import math
from sklearn import linear_model

import matplotlib.pyplot as plt


def tupleMaxValue(values):
    max = 0
    for element in values:
        if element > max:
            max = element
    max = int(max[0])
    return max


itemid = 220050
print "Script started."
# connection = None
try:
    connection = psycopg2.connect(
        host="localhost", database="mimic", user="ginger", password="source32")
    cursor = connection.cursor()
    print "Connected to the database."

    with open('systolic.csv', 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        print "CSV file opened."
        counter = 0
        for row in reader:
            numberOfMeasurements = row['numberOfMeasurements']
            subject_id = row['subject_id']
            minValue = 100
            maxValue = 150
            if int(numberOfMeasurements) >= minValue and int(numberOfMeasurements) <= maxValue:
                cursor.execute(
                "SELECT valuenum FROM result WHERE subject_id = %s AND itemid = %s ORDER BY charttime;", (subject_id, itemid))
                values = cursor.fetchall()
                train = []
                for i in range(len(values)):
                    train.append(i)
                print "Subject_id: ", subject_id
                n = len(values)
                print "Darabszam: ", n
                sum = 0
                for i in values:
                    i = int(i[0])
                    sum = sum + i
                average = (float(sum) / float(n))
                print "Atlag: ", average
                temp = 0.0
                for i in values:
                    i = int(i[0])
                    temp += pow((i - average), 2)
                dispersionSquare = (float(temp)) / n
                dispersion = math.sqrt(dispersionSquare)
                print "Szoras: ", dispersion
        
                regr = linear_model.LinearRegression()
                regr.fit(values, train)
                prediction = regr.predict(values)
                plt.plot(range(1, n + 1, 1), values, 'ro',
                         range(1, n + 1, 1), values, 'k')
                        
                plt.plot(prediction, values, color='blue', linewidth=3)
                plt.ylabel('Mert ertek')
                plt.xlabel('Meres sorszam')
                plt.xticks(range(1, n + 1))
                    
                max = tupleMaxValue(values)
                print "Legnagyobb ertek: ", max
                plt.yticks(range(0, max+1, 5))
                plt.grid(True)
                plt.show()
                print "-----------------------------"
                counter += 1
                if (counter == 20):
                    break;
except(Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if connection is not None:
        cursor.close()
        connection.close()
        print "Database connection closed."
    print "Script finished."
    exit()

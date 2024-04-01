
import csv
with open('nutrition_small.csv', mode= 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
        print(row['Question'])
print("yeah")
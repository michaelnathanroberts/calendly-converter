import csv
import datetime
import os
import time
import zipfile
from typing import Optional

import pandas as pd
from salesforce_bulk import CsvDictsAdapter
from salesforce_bulk import SalesforceBulk
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

thePassword = ... # Censored

# For Windows
SEPERATOR = "\\"

# For Mac
# SEPERATOR = "/"

CURRENT_DIR = os.getcwd()
DOWNLOADS = SEPERATOR.join(CURRENT_DIR.split(SEPERATOR)[0:3]) + SEPERATOR + "Downloads"


def dummyList(length):
    return [None] * length


def handleNan(obj):
    if isinstance(obj, float):
        try:
            int(obj)
        except ValueError:
            return None
    return obj


chrome_options = Options()
# chrome_options.add_argument("--headless")
# chrome_options.add_argument("--window-size=1920,1080")
service = ChromeService(ChromeDriverManager().install())

try:
    os.remove("events-export.csv")
except FileNotFoundError:
    # if file doesn't exist, no need to delete it
    pass

try:
    os.remove("sorted-events-export.csv")
except FileNotFoundError:
    # if file doesn't exist, no need to delete it
    pass

try:
    os.remove("final-events-export.csv")
except FileNotFoundError:
    # if file doesn't exist, no need to delete it
    pass

os.chdir(DOWNLOADS)

try:
    os.remove("events-export.zip")
except FileNotFoundError:
    # if file doesn't exist, no need to delete it
    pass

with webdriver.Chrome(service=service, options=chrome_options) as driver:
    driver.get("https://calendly.com/app/login?email=CENSORED&lang=en")
    # assert "Calendly" in driver.title

    elem = driver.find_element(By.NAME, "password")
    elem.click()
    elem.send_keys(thePassword)
    elem.send_keys(Keys.RETURN)
    time.sleep(10)

    EndDate = datetime.datetime.now().strftime('%Y-%m-%d')  # today
    StartDate = str(datetime.datetime.now() - datetime.timedelta(days=40))[0:10]

    driver.get(
        "https://calendly.com/app/scheduled_events/all_users_and_teams?period=fixed&status_ids%5B%5D=active&start_date="+str(StartDate)+"&end_date="+str(EndDate))
    driver.find_element(By.XPATH, "//*[@id=\"root\"]/main/div[2]/div/div/div[1]/div/div[2]/div[2]/button").click()
    time.sleep(10)

    print("Finished getting file")
# first last email phone company

# MAKE CHANGES HERE

with zipfile.ZipFile("events-export.zip") as zipRef:
    os.chdir(CURRENT_DIR)
    zipRef.extractall()
    dump = ""

df: pd.DataFrame = pd.read_csv("events-export.csv")
numQuestions: int = 0
questionsTables: list[dict[str, Optional[str]]] = []

for numQuestions in range(10):
    try:
        df.at[0, f'Question {numQuestions + 1}']
    except KeyError:
        break

for rowNumber in range(len(df)):
    questionsTable: dict[str, Optional[str]] = {}
    for i in range(numQuestions):
        question = handleNan(df.at[rowNumber, f'Question {i + 1}'])
        answer = handleNan(df.at[rowNumber, f'Response {i + 1}'])
        if question:
            questionsTable[question] = answer
    questionsTables.append(questionsTable)

for questionsTable in questionsTables:
    phoneNumberKey: Optional[str] = None
    companyKey: Optional[str] = None
    websiteKey: Optional[str] = None

    for key, value in questionsTable.items():
        lowerCasedKey = key.lower()
        if "phone" in lowerCasedKey:
            phoneNumberKey = phoneNumberKey or key
        elif "website" in lowerCasedKey:
            websiteKey = websiteKey or key
        elif "company" in lowerCasedKey or "firm" in lowerCasedKey \
                or "agency" in lowerCasedKey or "corporation" in lowerCasedKey:
            companyKey = companyKey or key

    newQuestionsTable: dict[str, Optional[str]] = {
        "Phone Number": None,
        "Company": None,
        "Website": None
    }
    if phoneNumberKey:
        newQuestionsTable["Phone Number"] = questionsTable[phoneNumberKey].replace('\'', '')
    if companyKey:
        newQuestionsTable["Company"] = questionsTable[companyKey]
    if websiteKey:
        newQuestionsTable["Website"] = questionsTable[websiteKey]

    for key, value in questionsTable.items():
        if isinstance(key, str) and key not in [phoneNumberKey, companyKey, websiteKey]:
            newQuestionsTable[key] = value

    questionsTable.clear()
    questionsTable.update(newQuestionsTable)

for i in range(3):
    lastResponseLoc = df.columns.get_loc(f'Response {numQuestions}')
    numQuestions += 1
    df.insert(lastResponseLoc + 1, f'Question {numQuestions}', dummyList(len(df)))
    df.insert(lastResponseLoc + 2, f'Response {numQuestions}', dummyList(len(df)))

for rowIndex in range(len(df)):
    questionsTable = questionsTables[rowIndex]
    questions = list(questionsTable.keys())
    for index in range(len(questions)):
        question = questions[index]
        answer = questionsTable[question]
        df.at[rowIndex, f'Question {index + 1}'] = question
        df.at[rowIndex, f'Response {index + 1}'] = answer

df.to_csv("sorted-events-export.csv", index=False)

with open("sorted-events-export.csv", 'r', encoding="utf-8") as csvFile:
    dump = csvFile.read()

    dump = dump.replace("Invitee ", "")
    dump = dump.replace("Response 1", "Phone")
    dump = dump.replace("Response 2", "Company")
    dump = dump.replace("First Name", "FirstName")
    dump = dump.replace("Last Name", "LastName")

    print("File edits complete")

with open("final-events-export.csv", 'w', encoding="utf-8") as newCsv:
    newCsv.write(dump)

f = pd.read_csv("final-events-export.csv")
keep_col = ['FirstName', "LastName", 'Email', 'Phone', "Company"]
new_f = f[keep_col]
new_f = new_f.fillna("Missing Data")

new_f.to_csv("final-events-export.csv", index=False)

print("Finished downloading file")

bulk = SalesforceBulk(security_token="GtUj9TRZpdkvYcH1nkoLo3OAS", username="racheli@gmail.com",
                      password='eaglepoint-1')

job = bulk.create_insert_job("Lead", contentType='CSV', concurrency='Parallel')

reader = csv.DictReader(open('final-events-export.csv', "r", encoding="utf-8"))
disbursals = []
for row in reader:
    disbursals.append(row)

csv_iter = CsvDictsAdapter(iter(disbursals))
batch = bulk.post_batch(job, csv_iter)
ans = bulk.get_batch_list(job)
for i in ans:
    while not bulk.is_batch_done(batch):
        time.sleep(10)  # wait for it to be done
# bulk.wait_for_batch(job, batch)
bulk.close_job(job)

print("Done. Data Uploaded.")



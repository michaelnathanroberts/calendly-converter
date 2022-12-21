import os
from typing import Optional

import pandas as pd


def dummyList(length):
    return [None] * length


def handleNan(obj):
    if isinstance(obj, float):
        try:
            int(obj)
        except ValueError:
            return None
    return obj


try:
    os.remove("sorted-events-export.csv")
except FileNotFoundError:
    # if file doesn't exist, no need to delete it
    pass

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



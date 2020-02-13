# ExchangeRate api to Mimic fixer.io
UPDATE: 2018-03-28: We found out that the fixer.io API is no more free for the needed full use:
* We can try to do an alternative version of that task (implement an API, download data into a well-designed database schema.)

* There is a way to solve the task with the limited capabilities of the free fixer.io API. But it takes more time, WE can not load years of data, so prepare the example for some months or weeks…

* Use exchangeratesapi.io. to get the exchange rate.

## Setup and run instructions
Install the requirements into a virtualenv or your environment of choice

    pip install -r requirements.txt

which includes
* flask
* Pandas
* pytest


from datetime import date, datetime, timedelta


def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return d + timedelta(days_ahead)


def generate_dates_from_weeks(number_of_weeks, enddate_str):
    if (enddate_str == ""):
        return []

    end_date = datetime.strptime(enddate_str, '%Y%m%d')
    date_list = []
    after_week_delta = timedelta(days=7)
    one_week_delta = timedelta(days=6)
    current_date = next_weekday(end_date, 6) - after_week_delta # we are looking for a sunday
    for i in range(number_of_weeks):
        right_date = current_date.strftime('%Y%m%d')
        print("generate_dates: right_date: iteration: "+ str(i) + " " + right_date)
        left_curr_date = current_date - one_week_delta
        left_date = left_curr_date.strftime('%Y%m%d')
        print("generate_dates: left_date: iteration: "+ str(i) + " " + left_date)
        date_list.append([left_date, right_date])
        current_date = current_date - after_week_delta
    return date_list


def generate_previous_week():
    td = date.today()
    two_week_delta = timedelta(days=14)
    prev_date = td - two_week_delta
    one_week_delta = timedelta(days=6)
    prev_monday = next_weekday(prev_date, 0)
    prev_sunday = prev_monday + one_week_delta
    return prev_monday.strftime('%Y%m%d'), prev_sunday.strftime('%Y%m%d')


def generate_previous_date():
    td = date.today()
    two_days_delta = timedelta(days=2)
    prev_date = td - two_days_delta
    return prev_date.strftime('%Y%m%d') 


def generate_previous_dates(number, date):
    current_date = datetime.strptime(date, '%Y%m%d')
    date_list = []
    for i in range(number):
        current_date = current_date - timedelta(days=1)
        date_list.append(current_date.strftime('%Y%m%d'))
    
    return date_list


def generate_previous_week_date():
    td = date.today()
    sev_days_delta = timedelta(days=7)
    prev_date = td - sev_days_delta
    return prev_date.strftime('%Y%m%d')


def generate_dates(number_of_weeks):
    current_date = datetime(year=2021, month=4, day=12, hour=0, minute=9, second=0)
    date_list = []
    after_week_delta = timedelta(days=7)
    one_week_delta = timedelta(days=6)
    for i in range(number_of_weeks):
        left_date = current_date.strftime('%Y%m%d')
        print("generate_dates: left_date: iteration: "+ str(i) + " " + left_date)
        right_curr_date = current_date + one_week_delta
        right_date = right_curr_date.strftime('%Y%m%d')
        print("generate_dates: right_date: iteration: "+ str(i) + " " + right_date) 
        date_list.append([left_date, right_date])
        current_date = current_date + after_week_delta
    return date_list


def generate_weeks(startdate_str, enddate_str):
    start_date = datetime.strptime(startdate_str, "%Y-%m-%d")
    current_date = next_weekday(start_date, 0) # we are looking for a monday
    date_list = []
    after_week_delta = timedelta(days=7)
    one_week_delta = timedelta(days=6)
    limit = (datetime.strptime(enddate_str, "%Y-%m-%d")).strftime('%Y%m%d')    
    
    right_date = ""
    while (right_date < limit):
        left_date = current_date.strftime('%Y%m%d')
        print("generate_weeks: left_date: iteration: " + left_date)
        right_curr_date = current_date + one_week_delta
        right_date = right_curr_date.strftime('%Y%m%d')
        print("generate_weeks: right_date: iteration: " + right_date) 
        date_list.append([left_date, right_date])
        current_date = current_date + after_week_delta
        
    return date_list


def first_day_of_month(date_time):
    date_str = date_time.strftime('%Y%m%d')
    month_id = date_str[:-2]
    first_day = month_id + '01'
    dico = {"month_id": month_id, "first_day": datetime.strptime(first_day, '%Y%m%d')}    
    return dico


def generate_months(startdate_str, enddate_str):
    start_date = datetime.strptime(startdate_str, "%Y-%m-%d")
    dico = first_day_of_month(start_date)
    current_month_date = dico["first_day"]
    current_month = dico["month_id"]
    current_month_date_str = current_month_date.strftime('%Y%m%d')
    date_list = []
    one_month_delta = timedelta(days=31)
    limit = (datetime.strptime(enddate_str, "%Y-%m-%d")).strftime('%Y%m%d')

    while (current_month_date_str < limit):
        date_list.append(current_month)
        print("generate_months: current_month: " + current_month) 
        current_date = current_month_date + one_month_delta
        dico = first_day_of_month(current_date)
        current_month = dico["month_id"]
        current_month_date = dico["first_day"]        
        current_month_date_str = current_month_date.strftime('%Y%m%d')

    return date_list


def generate_list_months(numbers, enddate_str):
    end_date = datetime.strptime(enddate_str, "%Y%m%d")
    dico = first_day_of_month(end_date)
    current_month_date = dico["first_day"]
    current_month = dico["month_id"]
    current_month_date_str = current_month_date.strftime('%Y%m%d')
    date_list = []
    one_month_delta = timedelta(days=31)

    for i in range(numbers):
        date_list.append(current_month)
        print("generate_months: current_month: " + current_month)
        current_date = current_month_date - one_month_delta
        dico = first_day_of_month(current_date)
        current_month = dico["month_id"]
        current_month_date = dico["first_day"]
        current_month_date_str = current_month_date.strftime('%Y%m%d')

    return date_list


def extreme_days_of_month(date_time):
    date_str = date_time.strftime('%Y%m%d')
    first_day = date_str[:-2] + '01'

    next_month = date_time.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)

    dico = {"first_day": datetime.strptime(first_day, '%Y%m%d'), "last_day": last_day}
    return dico


def generate_months_range(numbers, enddate_str):
    end_date = datetime.strptime(enddate_str, "%Y%m%d")
    dico = extreme_days_of_month(end_date)
    current_first_day = dico["first_day"]
    current_startdate_month_str = current_first_day.strftime('%Y%m%d')
    current_last_day = dico["last_day"]
    current_enddate_month_str = current_last_day.strftime('%Y%m%d')
    date_list = []
    delta = timedelta(days=1)

    for i in range(numbers):
        date_list.append([current_startdate_month_str, current_enddate_month_str])
        print("generate_months: current_month: " + current_startdate_month_str + " - " + current_enddate_month_str)
        current_date = current_first_day - delta
        dico = extreme_days_of_month(current_date)
        current_first_day = dico["first_day"]
        current_startdate_month_str = current_first_day.strftime('%Y%m%d')
        current_last_day = dico["last_day"]
        current_enddate_month_str = current_last_day.strftime('%Y%m%d')

    return date_list

